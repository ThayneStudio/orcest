import type {
  ConsumerGroupInfo,
  DashboardMessage,
  DashboardPolicy,
  DeadLetterEntry,
  LockInfo,
  QueuedTask,
  RecentResult,
  StuckTask,
  SystemSnapshot,
  WorkerPoolInfo,
} from "./types";
import { normalizeAttemptCountMap } from "./attempts";
import { normalizeDepth, normalizeQueueDepths } from "./queues";

const DEFAULT_DASHBOARD_POLICY: DashboardPolicy = {
  max_attempts: 3,
  pending_task_ttl_seconds: 16500,
  lock_ttl_seconds: 180,
};

function arrayOrEmpty<T>(value: T[] | undefined): T[] {
  return Array.isArray(value) ? value : [];
}

function recordOrEmpty<T>(value: Record<string, T> | undefined): Record<string, T> {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

function objectRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function stringValue(value: unknown, fallback = ""): string {
  if (typeof value !== "string") return fallback;
  const trimmed = value.trim();
  return trimmed || fallback;
}

function stringOrNumberValue(value: unknown): string {
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  return "";
}

function resourceIdValue(value: unknown, fallback = ""): string {
  const text = stringOrNumberValue(value).trim();
  return text || fallback;
}

function numericValue(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function nullableStringValue(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed || null;
}

function optionalNullableStringValue(
  record: Record<string, unknown>,
  key: string,
): string | null | undefined {
  if (!Object.prototype.hasOwnProperty.call(record, key)) return undefined;
  const value = record[key];
  if (value === null) return null;
  if (typeof value !== "string") return undefined;
  return value.trim() || null;
}

function optionalTrueValue(
  record: Record<string, unknown>,
  key: string,
): true | undefined {
  if (!Object.prototype.hasOwnProperty.call(record, key)) return undefined;
  return record[key] === true ? true : undefined;
}

function nullableStringOrNumberValue(value: unknown): string | null {
  const text = stringOrNumberValue(value).trim();
  return text || null;
}

function normalizeDurationSeconds(value: unknown): number {
  const parsed = numericValue(value);
  return parsed !== null && parsed >= 0 && Number.isInteger(parsed) ? parsed : -1;
}

function normalizeNullableDepth(value: unknown): number | null {
  const parsed = numericValue(value);
  return parsed !== null && parsed >= 0 ? Math.floor(parsed) : null;
}

function normalizeStringList(value: unknown): string[] {
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const item of arrayOrEmpty<unknown>(value as unknown[])) {
    const text = stringOrNumberValue(item).trim();
    if (!text || seen.has(text)) continue;
    seen.add(text);
    normalized.push(text);
  }
  return normalized;
}

function normalizeStringOnlyList(value: unknown): string[] {
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const item of arrayOrEmpty<unknown>(value as unknown[])) {
    if (typeof item !== "string") continue;
    const text = item.trim();
    if (!text || seen.has(text)) continue;
    seen.add(text);
    normalized.push(text);
  }
  return normalized;
}

function normalizeRecentResults(value: unknown): RecentResult[] {
  return arrayOrEmpty<unknown>(value as unknown[])
    .map((row, index) => {
      const r = objectRecord(row);
      if (!r) return null;
      const resultStream = stringValue(r.result_stream);
      const entryId = stringValue(r.entry_id);
      const resultId = stringValue(
        r.result_id,
        resultStream && entryId ? `${resultStream}:${entryId}` : `result:${index}`,
      );
      const normalized: RecentResult = {
        result_id: resultId,
        result_stream: resultStream,
        entry_id: entryId,
        task_id: stringValue(r.task_id),
        worker_id: stringValue(r.worker_id),
        status: stringValue(r.status),
        repo: nullableStringValue(r.repo),
        resource_type: stringValue(r.resource_type),
        resource_id: resourceIdValue(r.resource_id),
        duration_seconds: normalizeDurationSeconds(r.duration_seconds),
        summary: stringValue(r.summary),
      };
      const outputPrefix = optionalNullableStringValue(r, "output_prefix");
      if (outputPrefix !== undefined) normalized.output_prefix = outputPrefix;
      if (optionalTrueValue(r, "output_prefix_unresolved")) {
        normalized.output_prefix_unresolved = true;
      }
      return normalized;
    })
    .filter((row): row is RecentResult => row !== null);
}

function normalizeDeadLetters(value: unknown): DeadLetterEntry[] {
  return arrayOrEmpty<unknown>(value as unknown[])
    .map((row, index) => {
      const entry = objectRecord(row);
      if (!entry) return null;
      const deadLetterStream = stringValue(entry.dead_letter_stream);
      const entryId = stringValue(entry.entry_id);
      const deadLetterId = stringValue(
        entry.dead_letter_id,
        deadLetterStream && entryId ? `${deadLetterStream}:${entryId}` : `dead-letter:${index}`,
      );
      return {
        dead_letter_id: deadLetterId,
        dead_letter_stream: deadLetterStream,
        entry_id: entryId,
        task_id: stringValue(entry.task_id),
        task_type: stringValue(entry.task_type, "?"),
        repo: stringValue(entry.repo, "?"),
        resource_type: stringValue(entry.resource_type, "?"),
        resource_id: resourceIdValue(entry.resource_id, "?"),
        timestamp_ms: normalizeTimestampMs(entry.timestamp_ms),
        reason: nullableStringValue(entry.reason),
      };
    })
    .filter((row): row is DeadLetterEntry => row !== null);
}

function normalizeTimestampMs(value: unknown): number | null {
  const parsed = numericValue(value);
  return parsed !== null && parsed >= 0 && Number.isInteger(parsed) ? parsed : null;
}

function normalizeLocks(value: unknown): LockInfo[] {
  return arrayOrEmpty<unknown>(value as unknown[])
    .map((row, index) => {
      const lock = objectRecord(row);
      if (!lock) return null;
      const lockKey = stringValue(lock.lock_key, `lock:${index}`);
      const normalized: LockInfo = {
        lock_key: lockKey,
        prefix: nullableStringValue(lock.prefix),
        resource: stringValue(lock.resource),
        resource_type: stringValue(lock.resource_type),
        repo: stringValue(lock.repo),
        resource_id: resourceIdValue(lock.resource_id),
        owner: stringValue(lock.owner, "(expired)"),
        ttl: normalizeLockTtl(lock.ttl),
        task_id: nullableStringValue(lock.task_id),
        pending_created_at: nullableStringValue(lock.pending_created_at),
      };
      const outputPrefix = optionalNullableStringValue(lock, "output_prefix");
      if (outputPrefix !== undefined) normalized.output_prefix = outputPrefix;
      if (optionalTrueValue(lock, "output_prefix_unresolved")) {
        normalized.output_prefix_unresolved = true;
      }
      return normalized;
    })
    .filter((row): row is LockInfo => row !== null);
}

function normalizeLockTtl(value: unknown): number {
  const ttl = numericValue(value);
  return ttl !== null ? ttl : Number.NaN;
}

function normalizeConsumerGroups(value: unknown): ConsumerGroupInfo[] {
  return arrayOrEmpty<unknown>(value as unknown[])
    .map((row) => {
      const group = objectRecord(row);
      if (!group) return null;
      return {
        stream: stringValue(group.stream),
        name: stringValue(group.name, "?"),
        consumers: normalizeDepth(group.consumers),
        pending: normalizeDepth(group.pending),
        lag: normalizeNullableDepth(group.lag),
      };
    })
    .filter((row): row is ConsumerGroupInfo => row !== null);
}

function normalizeQueuedTasks(value: unknown): QueuedTask[] {
  return arrayOrEmpty<unknown>(value as unknown[])
    .map((row): QueuedTask | null => {
      const task = objectRecord(row);
      if (!task) return null;
      return {
        entry_id: stringValue(task.entry_id),
        task_id: stringValue(task.task_id),
        task_type: stringValue(task.task_type, "?"),
        prefix: nullableStringValue(task.prefix),
        repo: stringValue(task.repo, "?"),
        resource_type: stringValue(task.resource_type, "?"),
        resource_id: resourceIdValue(task.resource_id, "?"),
        created_at: nullableStringValue(task.created_at),
        stream: stringValue(task.stream),
      };
    })
    .filter((row): row is QueuedTask => row !== null);
}

function normalizeStuckTasks(value: unknown): StuckTask[] {
  return arrayOrEmpty<unknown>(value as unknown[])
    .map((row) => {
      const task = objectRecord(row);
      if (!task) return null;
      const severity = task.severity === "critical" ? "critical" : "warning";
      const normalized: StuckTask = {
        prefix: nullableStringValue(task.prefix),
        resource_type: stringValue(task.resource_type),
        repo: nullableStringValue(task.repo),
        resource_id: resourceIdValue(task.resource_id),
        reason: stringValue(task.reason),
        severity,
      };
      const stream = nullableStringValue(task.stream);
      const consumerGroup = nullableStringValue(task.consumer_group);
      const entryId = nullableStringValue(task.entry_id);
      const taskId = nullableStringValue(task.task_id);
      if (stream) normalized.stream = stream;
      if (consumerGroup) normalized.consumer_group = consumerGroup;
      if (entryId) normalized.entry_id = entryId;
      if (taskId) normalized.task_id = taskId;
      if (task.no_worker_consumers === true) normalized.no_worker_consumers = true;
      return normalized;
    })
    .filter((row): row is StuckTask => row !== null);
}

function normalizeWorkerPools(value: unknown): WorkerPoolInfo[] {
  const pools = arrayOrEmpty<unknown>(value as unknown[])
    .map((row) => {
      const pool = objectRecord(row);
      if (!pool) return null;
      const hasIdleList = Array.isArray(pool.idle);
      const idle = hasIdleList ? normalizeStringList(pool.idle) : [];
      const hasActiveList = Array.isArray(pool.active);
      const activeVmids = new Set<string>();
      const active = arrayOrEmpty<unknown>(pool.active as unknown[])
        .map((vm) => {
          const activeVm = objectRecord(vm);
          if (!activeVm) return null;
          const vmid = stringOrNumberValue(activeVm.vmid).trim();
          if (!vmid || activeVmids.has(vmid)) return null;
          activeVmids.add(vmid);
          return {
            vmid,
            started_at: nullableStringValue(activeVm.started_at),
            age_seconds: normalizeNullableNonNegativeInteger(activeVm.age_seconds),
          };
        })
        .filter((vm): vm is WorkerPoolInfo["active"][number] => vm !== null);
      return {
        prefix: stringValue(pool.prefix),
        template_vmid: nullableStringOrNumberValue(pool.template_vmid),
        idle,
        active,
        idle_count: hasIdleList ? idle.length : Math.max(idle.length, normalizeDepth(pool.idle_count)),
        active_count: hasActiveList
          ? active.length
          : Math.max(active.length, normalizeDepth(pool.active_count)),
      };
    })
    .filter((row): row is WorkerPoolInfo => row !== null);

  const poolsByPrefix = new Map<string, WorkerPoolInfo>();
  for (const pool of pools) {
    const existing = poolsByPrefix.get(pool.prefix);
    if (!existing) {
      poolsByPrefix.set(pool.prefix, pool);
      continue;
    }

    const idle = mergeStringLists(existing.idle, pool.idle);
    const active = mergeActiveVms(existing.active, pool.active);
    poolsByPrefix.set(pool.prefix, {
      prefix: pool.prefix,
      template_vmid: existing.template_vmid || pool.template_vmid,
      idle,
      active,
      idle_count: Math.max(existing.idle_count, pool.idle_count, idle.length),
      active_count: Math.max(existing.active_count, pool.active_count, active.length),
    });
  }

  return [...poolsByPrefix.values()];
}

function mergeStringLists(left: string[], right: string[]): string[] {
  const seen = new Set<string>();
  const merged: string[] = [];
  for (const value of [...left, ...right]) {
    if (seen.has(value)) continue;
    seen.add(value);
    merged.push(value);
  }
  return merged;
}

function mergeActiveVms(
  left: WorkerPoolInfo["active"],
  right: WorkerPoolInfo["active"],
): WorkerPoolInfo["active"] {
  const activeByVmid = new Map<string, WorkerPoolInfo["active"][number]>();
  for (const vm of [...left, ...right]) {
    const existing = activeByVmid.get(vm.vmid);
    if (!existing) {
      activeByVmid.set(vm.vmid, vm);
      continue;
    }
    activeByVmid.set(vm.vmid, {
      vmid: vm.vmid,
      started_at: existing.started_at || vm.started_at,
      age_seconds: minNullableNumber(existing.age_seconds, vm.age_seconds),
    });
  }
  return [...activeByVmid.values()];
}

function minNullableNumber(left: number | null, right: number | null): number | null {
  if (left === null) return right;
  if (right === null) return left;
  return Math.min(left, right);
}

function normalizeProviderHealth(value: unknown): SystemSnapshot["provider_health"] {
  const health: SystemSnapshot["provider_health"] = {};
  const providers = recordOrEmpty<unknown>(value as Record<string, unknown>);

  for (const [rawProvider, rawMetrics] of Object.entries(providers)) {
    const provider = rawProvider.trim();
    const metrics = recordOrEmpty<unknown>(rawMetrics as Record<string, unknown>);
    if (!provider || Object.keys(metrics).length === 0) continue;

    const normalizedMetrics = health[provider] || {};
    for (const [rawMetric, rawValue] of Object.entries(metrics)) {
      const metric = rawMetric.trim();
      const value = numericValue(rawValue);
      if (!metric || value === null || !Number.isInteger(value) || value < 0) continue;
      normalizedMetrics[metric] = Math.max(normalizedMetrics[metric] || 0, value);
    }

    if (Object.keys(normalizedMetrics).length > 0) {
      health[provider] = normalizedMetrics;
    }
  }

  return health;
}

function normalizeWorkers(value: unknown): string[] {
  const seen = new Set<string>();
  const workers: string[] = [];
  for (const worker of arrayOrEmpty<unknown>(value as unknown[])) {
    const id = stringOrNumberValue(worker).trim();
    if (!id || seen.has(id)) continue;
    seen.add(id);
    workers.push(id);
  }
  return workers;
}

function normalizePositiveInteger(value: unknown, fallback: number): number {
  const parsed = numericValue(value);
  return parsed !== null && Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

function normalizeNullableNonNegativeInteger(value: unknown): number | null {
  const parsed = numericValue(value);
  return parsed !== null && parsed >= 0 ? Math.floor(parsed) : null;
}

export function normalizeDashboardMessage(value: unknown): DashboardMessage | null {
  if (!value || typeof value !== "object") return null;

  const message = value as Partial<DashboardMessage>;
  if (!message.snapshot || typeof message.snapshot !== "object") return null;

  const snapshot = message.snapshot as Partial<SystemSnapshot>;
  const policy = snapshot.dashboard_policy || DEFAULT_DASHBOARD_POLICY;
  const recentResults = normalizeRecentResults(snapshot.recent_results);
  const deadLetterEntries = normalizeDeadLetters(snapshot.dead_letter_entries);
  const queuedTasks = normalizeQueuedTasks(snapshot.queued_tasks);

  return {
    snapshot: {
      redis_ok: snapshot.redis_ok === true,
      fetched_at: stringValue(snapshot.fetched_at),
      queue_depths: Object.fromEntries(
        normalizeQueueDepths(recordOrEmpty(snapshot.queue_depths))
          .map((row) => [row.name, row.depth]),
      ),
      results_depth: Math.max(normalizeDepth(snapshot.results_depth), recentResults.length),
      dead_letter_count: Math.max(
        normalizeDepth(snapshot.dead_letter_count),
        deadLetterEntries.length,
      ),
      locks: normalizeLocks(snapshot.locks),
      consumer_groups: normalizeConsumerGroups(snapshot.consumer_groups),
      recent_results: recentResults,
      attempt_counts: normalizeAttemptCountMap(recordOrEmpty(snapshot.attempt_counts)),
      dead_letter_entries: deadLetterEntries,
      queued_tasks: queuedTasks,
      provider_health: normalizeProviderHealth(snapshot.provider_health),
      worker_pool: normalizeWorkerPools(snapshot.worker_pool),
      degraded_sections: normalizeStringOnlyList(snapshot.degraded_sections),
      dashboard_policy: {
        max_attempts: normalizePositiveInteger(
          policy.max_attempts,
          DEFAULT_DASHBOARD_POLICY.max_attempts,
        ),
        pending_task_ttl_seconds: normalizePositiveInteger(
          policy.pending_task_ttl_seconds,
          DEFAULT_DASHBOARD_POLICY.pending_task_ttl_seconds,
        ),
        lock_ttl_seconds: normalizePositiveInteger(
          policy.lock_ttl_seconds,
          DEFAULT_DASHBOARD_POLICY.lock_ttl_seconds,
        ),
      },
    },
    stuck_tasks: normalizeStuckTasks(message.stuck_tasks),
    workers: normalizeWorkers(message.workers),
  };
}
