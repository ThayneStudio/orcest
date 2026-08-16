import { dashboardRedisKeyPatterns, redis, scanKeysMany } from "./redis.js";
import type { SystemSnapshot, StuckTask } from "./types.js";
import { parsePendingTaskMetadata } from "./snapshot.js";

const WORKER_CONSUMER_GROUP = "workers";
const PENDING_ENTRIES_PAGE_SIZE = 100;
const PENDING_ENTRIES_MAX_PAGES = 20;

class PendingEntriesTruncatedError extends Error {}

type PendingEntry = {
  key: string;
  lockKey: string;
  prefix: string | null;
  resourceType: string;
  repo: string;
  resourceId: string;
};

type PendingStreamTask = {
  prefix: string | null;
  resourceType: string | null;
  repo: string | null;
  resourceId: string | null;
  taskId: string | null;
};

type StuckTaskContext = {
  stream?: string | null;
  consumerGroup?: string | null;
  entryId?: string | null;
  taskId?: string | null;
  noWorkerConsumers?: boolean;
};

type WorkerGroupBacklogEvidence = {
  queueDepth: number;
  queuedPreview: number;
};

export async function detectStuck(snapshot: SystemSnapshot): Promise<StuckTask[]> {
  const stuck: StuckTask[] = [];
  const { max_attempts, pending_task_ttl_seconds, lock_ttl_seconds } =
    snapshot.dashboard_policy;
  const activeLocksComplete = !snapshot.degraded_sections.includes("active locks");
  const activeTaskIds = activeLocksComplete
    ? new Set(
      snapshot.locks
        .map((lock) => lock.task_id)
        .filter((taskId): taskId is string => Boolean(taskId)),
    )
    : new Set<string>();
  const activeResourceKeys = activeLocksComplete
    ? new Set(snapshot.locks.map(resourceKey).filter((key): key is string => Boolean(key)))
    : new Set<string>();
  const markDegraded = () => markSnapshotDegraded(snapshot, "stuck tasks");
  if (snapshot.degraded_sections.some((section) =>
    section === "consumer groups" ||
    section === "queued tasks" ||
    section === "queue depths"
  )) {
    markDegraded();
  }

  // 1. Orphaned pending: pending marker exists but no corresponding lock.
  // Keep this Redis-heavy check best-effort so snapshot-only signals below still
  // render if Redis errors while inspecting pending markers.
  try {
    const pendingKeys = await scanKeysMany(dashboardRedisKeyPatterns(["pending:*"]));

    // Parse all pending keys upfront so we can batch Redis calls
    const pendingEntries: PendingEntry[] = [];
    for (const key of pendingKeys) {
      const entry = pendingEntryFromKey(key);
      if (entry) pendingEntries.push(entry);
    }

    if (pendingEntries.length > 0) {
      // Batch all EXISTS + TTL calls into a single pipeline
      const lockPipeline = redis.pipeline();
      for (const { lockKey, key } of pendingEntries) {
        lockPipeline.exists(lockKey);
        lockPipeline.ttl(key);
        lockPipeline.get(key);
      }
      const results = await lockPipeline.exec();

      for (let i = 0; i < pendingEntries.length; i++) {
        const { prefix, resourceType, repo, resourceId } = pendingEntries[i];
        const offset = i * 3;
        const existsResult = results?.[offset];
        const ttlResult = results?.[offset + 1];
        const metadataResult = results?.[offset + 2];
        if (
          !existsResult ||
          !ttlResult ||
          !metadataResult ||
          pipelineResultError(existsResult) ||
          pipelineResultError(ttlResult) ||
          pipelineResultError(metadataResult)
        ) {
          markDegraded();
          continue;
        }
        const lockExists = pipelineNumber(existsResult) ?? 0;
        if (!lockExists) {
          const ttl = pipelineNumber(ttlResult) ?? -1;
          const pending = parsePendingTaskMetadata(metadataResult[1]);
          const pendingState = pendingTtlState(
            ttl,
            pending_task_ttl_seconds,
            lock_ttl_seconds,
            pending?.createdAt ?? null,
          );
          if (pendingState) {
            stuck.push(stuckTaskWithContext({
              prefix,
              resource_type: resourceType,
              repo,
              resource_id: resourceId,
              reason: pendingTtlReason(ttl, pending_task_ttl_seconds, pending?.createdAt ?? null),
              severity: pendingState,
            }, queuedTaskContext(snapshot, {
              prefix,
              resourceType,
              repo,
              resourceId,
              taskId: pending?.taskId ?? null,
            })));
          }
        }
      }
    }
  } catch {
    markDegraded();
    // Snapshot-derived stuck checks below still run.
  }

  // 2. Active locks that lost their expiry can block a resource indefinitely.
  for (const lock of snapshot.locks) {
    if (lock.ttl !== -1) continue;
    stuck.push({
      prefix: lock.prefix,
      resource_type: lock.resource_type,
      repo: lock.repo,
      resource_id: lock.resource_id,
      reason: "Lock has no TTL — resource can remain blocked indefinitely",
      severity: "critical",
    });
  }

  // 3. High attempt counts
  for (const [label, count] of Object.entries(snapshot.attempt_counts)) {
    const attempt = parseAttemptLabel(label);
    const severity = attemptStuckSeverity(count, max_attempts);
    if (severity === "critical") {
      stuck.push({
        prefix: attempt.prefix,
        resource_type: attempt.resourceType,
        repo: attempt.repo,
        resource_id: attempt.resourceId,
        reason: `Attempt count at max (${count}/${max_attempts})`,
        severity: "critical",
      });
    } else if (severity === "warning") {
      stuck.push({
        prefix: attempt.prefix,
        resource_type: attempt.resourceType,
        repo: attempt.repo,
        resource_id: attempt.resourceId,
        reason: `Attempt count near max (${count}/${max_attempts})`,
        severity: "warning",
      });
    }
  }

  // 4. Worker groups with retained backlog but no consumers cannot drain.
  for (const task of noConsumerBacklogStuckTasks(snapshot)) {
    stuck.push(task);
  }

  // 5. Stale consumer group entries (pending entries with high idle time and no active lock)
  const idleThresholdMs = Math.max(1, lock_ttl_seconds * 1000);
  if (!activeLocksComplete) {
    if (snapshot.consumer_groups.some((group) =>
      group.name === WORKER_CONSUMER_GROUP && group.pending > 0
    )) {
      markDegraded();
    }
    return suppressDuplicateNoConsumerQueueWarnings(stuck);
  }
  for (const group of snapshot.consumer_groups) {
    if (group.name !== WORKER_CONSUMER_GROUP) continue;
    if (workerGroupHasNoConsumersWhileBacklogged(
      group,
      workerGroupBacklogEvidence(snapshot, group.stream),
    )) continue;
    if (group.pending === 0) continue;
    try {
      const details = await pendingEntriesIdleAtLeast(
        group.stream,
        group.name,
        idleThresholdMs,
      );

      for (const entry of details) {
        if (!Array.isArray(entry) || entry.length < 4) continue;
        const entryId = String(entry[0] ?? "");
        const idleMs = nonNegativeInteger(entry[2]);
        const deliveryCount = nonNegativeInteger(entry[3]);
        if (!entryId || idleMs === null || deliveryCount === null) {
          continue;
        }

        if (idleMs >= idleThresholdMs) {
          const task = await pendingStreamTask(group.stream, entryId);
          if (!task) continue;
          if (task.taskId && activeTaskIds.has(task.taskId)) {
            continue;
          }
          const taskResourceKey = resourceKey({
            prefix: task.prefix,
            resource_type: task.resourceType,
            repo: task.repo,
            resource_id: task.resourceId,
          });
          if (taskResourceKey && activeResourceKeys.has(taskResourceKey)) {
            continue;
          }
          stuck.push(stuckTaskWithContext({
            prefix: task.prefix,
            resource_type: task.resourceType ?? "stream",
            repo: task.repo,
            resource_id: task.resourceId ?? `${group.stream}/${group.name}/${entryId}`,
            reason: `Pending entry idle for ${Math.round(idleMs / 1000)}s (${deliveryCount} deliveries)`,
            severity: deliveryCount >= 2 ? "critical" : "warning",
          }, {
            stream: group.stream,
            consumerGroup: group.name,
            entryId,
            taskId: task.taskId,
            noWorkerConsumers: workerGroupHasNoConsumersWhileBacklogged(group),
          }));
        }
      }
    } catch {
      markDegraded();
    }
  }

  return suppressDuplicateNoConsumerQueueWarnings(stuck);
}

function noConsumerBacklogStuckTasks(snapshot: SystemSnapshot): StuckTask[] {
  const stuck: StuckTask[] = [];
  for (const group of snapshot.consumer_groups) {
    const queuedTasks = snapshot.queued_tasks.filter((task) => task.stream === group.stream);
    const backlog = workerGroupBacklogEvidence(snapshot, group.stream, queuedTasks.length);
    if (!workerGroupHasNoConsumersWhileBacklogged(group, backlog)) continue;

    if (queuedTasks.length === 0) {
      stuck.push(stuckTaskWithContext({
        prefix: null,
        resource_type: "stream",
        repo: null,
        resource_id: `${group.stream}/${group.name}`,
        reason: noWorkerConsumersReason(group, backlog),
        severity: "critical",
      }, {
        stream: group.stream,
        consumerGroup: group.name,
        noWorkerConsumers: true,
      }));
      continue;
    }

    const seenResources = new Set<string>();
    for (const queued of queuedTasks) {
      const key = resourceKey({
        prefix: queued.prefix ?? null,
        resource_type: queued.resource_type,
        repo: queued.repo,
        resource_id: queued.resource_id,
      }) ?? JSON.stringify([queued.stream, queued.entry_id]);
      if (seenResources.has(key)) continue;
      seenResources.add(key);

      stuck.push(stuckTaskWithContext({
        prefix: queued.prefix ?? null,
        resource_type: queued.resource_type,
        repo: queued.repo,
        resource_id: queued.resource_id,
        reason: noWorkerConsumersReason(group, backlog),
        severity: "critical",
      }, {
        stream: group.stream,
        consumerGroup: group.name,
        entryId: queued.entry_id,
        taskId: queued.task_id,
        noWorkerConsumers: true,
      }));
    }
  }

  return stuck;
}

function noWorkerConsumersReason(group: {
  pending: number;
  lag: number | null;
}, backlog: WorkerGroupBacklogEvidence = { queueDepth: 0, queuedPreview: 0 }): string {
  const pending = nonNegativeInteger(group.pending) ?? 0;
  const lag = group.lag === null ? null : nonNegativeInteger(group.lag) ?? 0;
  const queueDepth = nonNegativeInteger(backlog.queueDepth) ?? 0;
  const queuedPreview = nonNegativeInteger(backlog.queuedPreview) ?? 0;
  const knownRedisBacklog = pending + (lag ?? 0);
  const redisBacklog = knownRedisBacklog > 0;
  const parts = [
    pending > 0 ? `${pending} pending` : "",
    lag === null ? "lag unknown" : `${lag} lag`,
    queueDepth > 0 && (!redisBacklog || queueDepth > knownRedisBacklog)
      ? `${queueDepth} queued`
      : "",
    queueDepth === 0 && queuedPreview > 0 && (lag === null || !redisBacklog)
      ? `${queuedPreview} queued preview`
      : "",
  ].filter(Boolean);
  return parts.length > 0
    ? `Worker group has backlog but no consumers (${parts.join(", ")})`
    : "Worker group has backlog but no consumers";
}

function stuckTaskWithContext(
  task: StuckTask,
  context: StuckTaskContext | null = null,
): StuckTask {
  if (!context) return task;
  const enriched = { ...task };
  const stream = stringField(context.stream);
  const consumerGroup = stringField(context.consumerGroup);
  const entryId = stringField(context.entryId);
  const taskId = stringField(context.taskId);
  if (stream) enriched.stream = stream;
  if (consumerGroup) enriched.consumer_group = consumerGroup;
  if (entryId) enriched.entry_id = entryId;
  if (taskId) enriched.task_id = taskId;
  if (context.noWorkerConsumers === true) enriched.no_worker_consumers = true;
  return enriched;
}

function stuckTaskQueueDuplicateKeys(task: StuckTask): string[] {
  const stream = stringField(task.stream);
  if (!stream) return [];
  const keys: string[] = [];
  const taskId = stringField(task.task_id);
  const entryId = stringField(task.entry_id);
  if (taskId) keys.push(`${stream}\0task\0${taskId}`);
  if (entryId) keys.push(`${stream}\0entry\0${entryId}`);
  const key = resourceKey({
    prefix: task.prefix,
    resource_type: task.resource_type,
    repo: task.repo,
    resource_id: task.resource_id,
  });
  if (key) keys.push(`${stream}\0resource\0${key}`);
  return keys;
}

function noConsumerCriticalReason(reason: string): boolean {
  return reason.trim().startsWith("Worker group has backlog but no consumers");
}

function queuedPickupWarningReason(reason: string): boolean {
  return reason.trim().startsWith("Queued but no worker has picked it up");
}

function suppressDuplicateNoConsumerQueueWarnings(tasks: StuckTask[]): StuckTask[] {
  const noConsumerCriticalKeys = new Set(
    tasks
      .filter((task) =>
        task.severity === "critical" &&
        task.no_worker_consumers &&
        noConsumerCriticalReason(task.reason)
      )
      .flatMap(stuckTaskQueueDuplicateKeys),
  );
  if (noConsumerCriticalKeys.size === 0) return tasks;

  return tasks.filter((task) => {
    if (
      task.severity !== "warning" ||
      !task.no_worker_consumers ||
      !queuedPickupWarningReason(task.reason)
    ) {
      return true;
    }
    const keys = stuckTaskQueueDuplicateKeys(task);
    return keys.length === 0 || keys.every((key) => !noConsumerCriticalKeys.has(key));
  });
}

function comparable(value: string | null | undefined): string {
  return (value || "").trim().toLowerCase();
}

function queuedTaskContext(
  snapshot: SystemSnapshot,
  resource: {
    prefix: string | null;
    resourceType: string;
    repo: string;
    resourceId: string;
    taskId: string | null;
  },
): StuckTaskContext | null {
  const taskId = comparable(resource.taskId);
  const candidates = snapshot.queued_tasks.filter((task) =>
    resourcePrefixesCompatible(resource.prefix, task.prefix ?? null)
  );
  // queued_tasks is accumulated stream by stream, so with provider-isolated
  // streams a stale same-repo entry on one stream would otherwise win over the
  // exact task_id match on another. Match on task_id first, always.
  const queued = (taskId
    ? candidates.find((task) => comparable(task.task_id) === taskId)
    : undefined) ??
    candidates.find((task) =>
      comparable(task.resource_type) === comparable(resource.resourceType) &&
      comparable(task.repo) === comparable(resource.repo) &&
      comparable(task.resource_id) === comparable(resource.resourceId)
    );
  if (!queued) return null;

  const workerGroup = snapshot.consumer_groups.find((group) =>
    group.stream === queued.stream && group.name === WORKER_CONSUMER_GROUP
  );
  const backlog = workerGroup
    ? workerGroupBacklogEvidence(snapshot, queued.stream)
    : { queueDepth: 0, queuedPreview: 0 };
  return {
    stream: queued.stream,
    consumerGroup: workerGroup?.name ?? null,
    entryId: queued.entry_id,
    taskId: queued.task_id || resource.taskId,
    noWorkerConsumers: workerGroup
      ? workerGroupHasNoConsumersWhileBacklogged(workerGroup, backlog)
      : false,
  };
}

function resourcePrefixesCompatible(
  expectedPrefix: string | null | undefined,
  candidatePrefix: string | null | undefined,
): boolean {
  const expected = comparable(expectedPrefix);
  const candidate = comparable(candidatePrefix);
  if (expected && candidate) return expected === candidate;
  if (expected && !candidate) return true;
  if (!expected && candidate) return false;
  return true;
}

function workerGroupHasNoConsumersWhileBacklogged(group: {
  name: string;
  consumers: number;
  pending: number;
  lag: number | null;
}, backlog: WorkerGroupBacklogEvidence = { queueDepth: 0, queuedPreview: 0 }): boolean {
  if (group.name !== WORKER_CONSUMER_GROUP || group.consumers !== 0) return false;
  return group.pending > 0 ||
    (group.lag !== null && group.lag > 0) ||
    backlog.queueDepth > 0 ||
    backlog.queuedPreview > 0;
}

function workerGroupBacklogEvidence(
  snapshot: SystemSnapshot,
  stream: string,
  queuedPreview?: number,
): WorkerGroupBacklogEvidence {
  const queueDepth = nonNegativeInteger(snapshot.queue_depths[stream]) ?? 0;
  return {
    queueDepth,
    queuedPreview: queuedPreview ?? snapshot.queued_tasks.filter((task) => task.stream === stream).length,
  };
}

function markSnapshotDegraded(snapshot: SystemSnapshot, section: string): void {
  if (!snapshot.degraded_sections.includes(section)) {
    snapshot.degraded_sections.push(section);
  }
}

function pipelineResultError(result: [unknown, unknown]): boolean {
  return Boolean(result[0]);
}

function pipelineNumber(result: [unknown, unknown]): number | null {
  return typeof result[1] === "number" && Number.isFinite(result[1]) ? result[1] : null;
}

function nonNegativeInteger(value: unknown): number | null {
  if (typeof value !== "number" && typeof value !== "string") return null;
  const trimmed = typeof value === "string" ? value.trim() : value;
  if (trimmed === "") return null;
  const parsed = Number(trimmed);
  return Number.isInteger(parsed) && parsed >= 0 ? parsed : null;
}

function resourceKey(resource: {
  prefix: string | null;
  resource_type: string | null;
  repo: string | null;
  resource_id: string | null;
}): string | null {
  if (!resource.resource_type || !resource.repo || !resource.resource_id) {
    return null;
  }
  return [
    resource.prefix ?? "",
    resource.resource_type,
    resource.repo,
    resource.resource_id,
  ].join("\0");
}

async function pendingEntriesIdleAtLeast(
  stream: string,
  group: string,
  idleThresholdMs: number,
): Promise<unknown[][]> {
  try {
    return await pendingEntriesRange(stream, group, idleThresholdMs);
  } catch (err) {
    if (err instanceof PendingEntriesTruncatedError) {
      throw err;
    }
    return await pendingEntriesRange(stream, group);
  }
}

async function pendingEntriesRange(
  stream: string,
  group: string,
  idleThresholdMs?: number,
): Promise<unknown[][]> {
  const rows: unknown[][] = [];
  let start = "-";

  for (let page = 0; page < PENDING_ENTRIES_MAX_PAGES; page++) {
    const result = idleThresholdMs === undefined
      ? await redis.call(
        "XPENDING",
        stream,
        group,
        start,
        "+",
        String(PENDING_ENTRIES_PAGE_SIZE),
      ) as unknown[][]
      : await redis.call(
        "XPENDING",
        stream,
        group,
        "IDLE",
        idleThresholdMs,
        start,
        "+",
        String(PENDING_ENTRIES_PAGE_SIZE),
      ) as unknown[][];
    if (!Array.isArray(result) || result.length === 0) break;

    rows.push(...result);
    if (result.length < PENDING_ENTRIES_PAGE_SIZE) break;

    const last = result[result.length - 1];
    const lastId = Array.isArray(last) ? String(last[0] ?? "") : "";
    const nextStart = nextRedisStreamId(lastId);
    if (!nextStart) {
      throw new PendingEntriesTruncatedError(
        `XPENDING result for ${stream}/${group} had an unpageable last id`,
      );
    }
    start = nextStart;

    if (page === PENDING_ENTRIES_MAX_PAGES - 1) {
      throw new PendingEntriesTruncatedError(
        `XPENDING result for ${stream}/${group} exceeded ${PENDING_ENTRIES_MAX_PAGES} pages`,
      );
    }
  }

  return rows;
}

function nextRedisStreamId(id: string): string | null {
  const match = id.match(/^(\d+)-(\d+)$/);
  if (!match) return null;
  return `${match[1]}-${BigInt(match[2]) + 1n}`;
}

async function pendingStreamTask(stream: string, entryId: string): Promise<PendingStreamTask | null> {
  const rows = await redis.xrange(stream, entryId, entryId) as unknown[];
  const fields = streamEntryFields(rows);
  return {
    prefix: stringField(fields.key_prefix) || prefixFromStream(stream),
    resourceType: stringField(fields.resource_type),
    repo: stringField(fields.repo),
    resourceId: stringField(fields.resource_id),
    taskId: stringField(fields.id),
  };
}

function streamEntryFields(rows: unknown[]): Record<string, unknown> {
  const row = Array.isArray(rows) ? rows[0] : null;
  if (!Array.isArray(row) || row.length < 2) return {};
  const rawFields = row[1];
  if (Array.isArray(rawFields)) {
    const fields: Record<string, unknown> = {};
    for (let i = 0; i < rawFields.length; i += 2) {
      fields[String(rawFields[i])] = rawFields[i + 1];
    }
    return fields;
  }
  if (rawFields && typeof rawFields === "object") {
    return rawFields as Record<string, unknown>;
  }
  return {};
}

function stringField(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function prefixFromStream(stream: string): string | null {
  const marker = ":tasks:";
  const index = stream.indexOf(marker);
  return index > 0 ? stream.slice(0, index) : null;
}

export function pendingEntryFromKey(key: string): PendingEntry | null {
  const prefixed = key.match(/^(.+):pending:(pr|issue):(.+):([^:]+)$/);
  const unprefixed = key.match(/^pending:(pr|issue):(.+):([^:]+)$/);
  const prefix = prefixed?.[1] ?? "";
  const resourceType = prefixed?.[2] ?? unprefixed?.[1];
  const repo = prefixed?.[3] ?? unprefixed?.[2];
  const resourceId = prefixed?.[4] ?? unprefixed?.[3];

  if (!resourceType || !repo || !resourceId) return null;

  const lockBase = `lock:${resourceType}:${repo}:${resourceId}`;
  return {
    key,
    lockKey: prefix ? `${prefix}:${lockBase}` : lockBase,
    prefix: prefix || null,
    resourceType,
    repo,
    resourceId,
  };
}

export function parseAttemptLabel(
  label: string,
): {
  prefix: string | null;
  resourceType: "pr" | "issue";
  repo: string | null;
  resourceId: string;
} {
  const match = label.match(/^(?:\[([^\]]+)\] )?(.+) (PR|Issue) #(\d+)$/);
  if (!match) return { prefix: null, resourceType: "pr", repo: null, resourceId: label };
  return {
    prefix: match[1] || null,
    resourceType: match[3] === "Issue" ? "issue" : "pr",
    repo: match[2],
    resourceId: match[4],
  };
}

export function attemptStuckSeverity(
  count: number,
  maxAttempts: number,
): StuckTask["severity"] | null {
  if (!Number.isFinite(count) || count <= 0) return null;
  if (count >= maxAttempts) return "critical";
  if (maxAttempts > 1 && count >= maxAttempts - 1) return "warning";
  return null;
}

export function pendingTtlState(
  ttlSeconds: number,
  expectedTtlSeconds: number,
  lockTtlSeconds: number,
  createdAt: string | null = null,
  nowMs = Date.now(),
): StuckTask["severity"] | null {
  if (ttlSeconds === -1) return "critical";
  if (ttlSeconds <= 0) return null;

  const warningAfterSeconds = Math.max(60, lockTtlSeconds);
  const ageSeconds = pendingAgeSeconds(createdAt, nowMs);
  if (ageSeconds !== null) {
    if (ageSeconds >= warningAfterSeconds) return "warning";
    return null;
  }

  if (expectedTtlSeconds <= 0) return null;

  // Legacy pending markers have no created_at. In that case, inferring age from
  // expected TTL is only trustworthy when the observed TTL is close to the
  // configured initial TTL. If the dashboard expects a much larger TTL than the
  // orchestrator actually set, an otherwise fresh marker would look old.
  // This guard has to run before every TTL-derived inference below: when it sat
  // between the ratio checks and the elapsed check it only applied inside the
  // (25%, 50%) band, which made severity non-monotonic in age (a marker went
  // warning -> null -> warning -> critical as it aged) and still let a fresh
  // marker be flagged critical whenever the policy TTL disagreed with reality.
  if (ttlSeconds < Math.ceil(expectedTtlSeconds * 0.5)) return null;

  if (ttlSeconds <= Math.ceil(expectedTtlSeconds * 0.1)) return "critical";
  if (ttlSeconds <= Math.ceil(expectedTtlSeconds * 0.25)) return "warning";

  const elapsedSeconds = Math.max(0, expectedTtlSeconds - ttlSeconds);
  if (elapsedSeconds >= warningAfterSeconds) return "warning";
  return null;
}

export function pendingAgeSeconds(createdAt: string | null, nowMs = Date.now()): number | null {
  if (!createdAt) return null;
  const createdMs = new Date(createdAt).getTime();
  if (!Number.isFinite(createdMs)) return null;
  return Math.max(0, Math.floor((nowMs - createdMs) / 1000));
}

function pendingTtlReason(
  ttlSeconds: number,
  expectedTtlSeconds: number,
  createdAt: string | null,
): string {
  if (ttlSeconds === -1) return "Queued with no TTL — pending marker will never expire";
  const ageSeconds = pendingAgeSeconds(createdAt);
  if (ageSeconds !== null) {
    return `Queued but no worker has picked it up (age: ${ageSeconds}s, pending TTL: ${ttlSeconds}s)`;
  }
  return `Queued but no worker has picked it up (pending TTL: ${ttlSeconds}s/${expectedTtlSeconds}s)`;
}
