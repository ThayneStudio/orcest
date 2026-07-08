import { dashboardRedisKeyPatterns, redis, scanKeysMany } from "./redis.js";

const ANSI_RE = /\x1b(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))/g;
const CONTROL_RE = /[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/g;
const BOX_DRAWING_ONLY_RE = /^[\s─━│┃┌┐└┘├┤┬┴┼╭╮╰╯═╔╗╚╝╠╣╦╩╬╟╢╤╧╪╷╵╴╶╸╺]+$/u;
const REDIS_STREAM_ID_RE = /^\d+-\d+$/;
const TASK_START_SCAN_BATCH_SIZE = 500;
const TASK_START_SCAN_MAX_ENTRIES = 50000;
const FORMATTED_JSON_LINE_MAX_CHARS = 4000;
const TRUNCATED_SUFFIX = "\n[truncated]";

/**
 * Return worker IDs that have output streams with recent activity.
 */
export class WorkerDiscoveryPartialError extends Error {
  readonly workers: string[];

  constructor(message: string, workers: string[]) {
    super(message);
    this.name = "WorkerDiscoveryPartialError";
    this.workers = workers;
  }
}

export class TaskOutputPartialReadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "TaskOutputPartialReadError";
  }
}

export async function discoverWorkers(): Promise<string[]> {
  const streams = await scanKeysMany(dashboardRedisKeyPatterns(["output:*"]));
  const workers: Array<{ id: string; lastEntryMs: number }> = [];
  let inspectedWorkerStreams = 0;
  let firstStreamError: Error | null = null;

  if (streams.length > 0) {
    // Pipeline all XREVRANGE COUNT 1 calls to avoid N sequential round-trips
    const pipeline = redis.pipeline();
    for (const stream of streams) pipeline.xrevrange(stream, "+", "-", "COUNT", 1);
    const results = await pipeline.exec();

    for (let i = 0; i < streams.length; i++) {
      const stream = streams[i];
      const workerId = workerIdFromOutputStream(stream);
      if (!workerId) continue;
      const result = results?.[i] as [Error | null, [string, string[]][]] | undefined;
      if (!result) {
        firstStreamError ??= new Error(`Worker discovery did not return a result for output stream ${stream}`);
        continue;
      }
      const [err, entries] = result;
      if (err) {
        const detail = err instanceof Error ? err.message : String(err);
        firstStreamError ??= new Error(`Failed to inspect worker output stream ${stream}: ${detail}`);
        continue;
      }
      if (!Array.isArray(entries)) {
        firstStreamError ??= new Error(`Worker discovery returned malformed entries for output stream ${stream}`);
        continue;
      }
      inspectedWorkerStreams++;
      if (entries && entries.length > 0) {
        const entryId = entries[0][0];
        const ms = streamTimestampMs(entryId);
        if (ms === null) continue;
        workers.push({ id: workerId, lastEntryMs: ms });
      }
    }
  }

  const discoveredWorkers = normalizeDiscoveredWorkers(workers);
  if (firstStreamError) {
    if (inspectedWorkerStreams === 0) {
      throw firstStreamError;
    }
    throw new WorkerDiscoveryPartialError(firstStreamError.message, discoveredWorkers);
  }

  return discoveredWorkers;
}

export function normalizeDiscoveredWorkers(
  workers: Array<{ id: string; lastEntryMs: number }>,
  nowMs = Date.now(),
): string[] {
  const cutoff = nowMs - 7 * 24 * 60 * 60 * 1000;
  const newestById = new Map<string, number>();

  for (const worker of workers) {
    if (!worker.id || !Number.isFinite(worker.lastEntryMs) || worker.lastEntryMs <= cutoff) {
      continue;
    }
    const current = newestById.get(worker.id);
    if (current === undefined || worker.lastEntryMs > current) {
      newestById.set(worker.id, worker.lastEntryMs);
    }
  }

  return [...newestById.entries()]
    .sort(([aId, aMs], [bId, bMs]) => bMs - aMs || aId.localeCompare(bId))
    .map(([id]) => id);
}

/**
 * Find the prefixed stream key for a worker (e.g., orcest:output:worker-1).
 */
// Cache resolved stream keys with TTL — worker stream names rarely change
// Capped at 100 entries to prevent unbounded growth from invalid worker IDs
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes
const STREAM_CACHE_MAX = 100;
const streamCache = new Map<string, { key: string; cachedAt: number }>();
const TASK_OUTPUT_PREFIX_CACHE_MAX = 500;
const taskOutputPrefixCache = new Map<
  string,
  { prefix: string | null; cachedAt: number }
>();

type RedisPrefixFilter = string | null | undefined;
export type TaskOutputPrefixLookup = {
  workerId: string;
  taskId: string;
};

export type TaskOutputPrefixLookupResult = {
  prefixes: Map<string, string | null | undefined>;
  degraded: boolean;
};

export function outputStreamPrefix(stream: string): string | null | undefined {
  const prefixed = stream.match(/^(.+):output:.+$/);
  if (prefixed) return prefixed[1];
  if (/^output:.+$/.test(stream)) return null;
  return undefined;
}

export function taskOutputPrefixLookupKey(workerId: string, taskId: string): string {
  return `${workerId.trim()}\0${taskId.trim()}`;
}

async function findWorkerStreams(
  workerId: string,
  redisPrefix?: RedisPrefixFilter,
): Promise<string[]> {
  const normalizedWorkerId = workerId.trim();
  if (!normalizedWorkerId) return [];

  if (redisPrefix !== undefined) {
    return [outputStreamForPrefix(normalizedWorkerId, redisPrefix)];
  }

  const streams = await scanKeysMany(dashboardRedisKeyPatterns(["output:*"]));
  return streams.filter((stream) => {
    if (workerIdFromOutputStream(stream) !== normalizedWorkerId) return false;
    return redisPrefix === undefined || outputStreamPrefix(stream) === redisPrefix;
  });
}

function outputStreamForPrefix(workerId: string, redisPrefix: string | null): string {
  return redisPrefix === null ? `output:${workerId}` : `${redisPrefix}:output:${workerId}`;
}

async function findWorkerStream(
  workerId: string,
  redisPrefix?: RedisPrefixFilter,
): Promise<string | null> {
  const cacheKey = `${redisPrefix === undefined ? "*" : redisPrefix ?? ""}\0${workerId}`;
  const cached = streamCache.get(cacheKey);
  if (cached && Date.now() - cached.cachedAt < CACHE_TTL) return cached.key;
  try {
    const matches = await findWorkerStreams(workerId, redisPrefix);
    if (matches.length > 0) {
      if (streamCache.size >= STREAM_CACHE_MAX) {
        const firstKey = streamCache.keys().next().value;
        if (firstKey !== undefined) streamCache.delete(firstKey);
      }
      streamCache.set(cacheKey, { key: matches[0], cachedAt: Date.now() });
      return matches[0];
    }
    return null;
  } catch {
    return null;
  }
}

export function workerIdFromOutputStream(stream: string): string | null {
  const prefixed = stream.match(/^.+:output:(.+)$/);
  const unprefixed = stream.match(/^output:(.+)$/);
  const workerId = prefixed?.[1] ?? unprefixed?.[1] ?? null;
  return workerId && workerId.trim() ? workerId : null;
}

export function taskOutputUnavailableMessage(taskId: string): string {
  const displayTaskId = taskId.trim() || "unknown task";
  return `Output for task ${displayTaskId} was not found. The worker output stream may have been trimmed or the task may have finished before output capture started.`;
}

export function taskOutputReadFailureMessage(): string {
  return "Task output could not be read from Redis. Check dashboard Redis connectivity and worker output streams.";
}

export function normalizeTaskOutputCursor(cursor: string | null | undefined): string {
  const trimmed = (cursor || "").trim();
  return REDIS_STREAM_ID_RE.test(trimmed) ? trimmed : "0-0";
}

function streamTimestampMs(entryId: string): number | null {
  return streamIdParts(entryId)?.[0] ?? null;
}

/**
 * Cache resolved task start IDs — once found, the entry ID doesn't change.
 * Key: `${workerId}:${taskId}` (never caches the "latest" case)
 * Capped at 500 entries to prevent unbounded growth.
 */
const TASK_START_CACHE_MAX = 500;
type TaskStartLocation = {
  stream: string;
  entryId: string;
};
const taskStartCache = new Map<string, TaskStartLocation>();

export function clearWorkerCachesForTesting(): void {
  streamCache.clear();
  taskStartCache.clear();
  taskOutputPrefixCache.clear();
}

/**
 * Find the stream entry ID where a task started on a given worker.
 * Scans backward from the end of the stream looking for a task_start marker.
 * For exact task lookups, tagged output is also accepted because worker
 * task_start publication is best-effort while output lines carry task_id.
 * If taskId is omitted, returns the most recent task_start marker.
 */
export async function findTaskStart(
  workerId: string,
  taskId?: string,
  redisPrefix?: RedisPrefixFilter,
): Promise<TaskStartLocation | null> {
  // Don't cache the "latest" case — a new task may have started since last lookup
  const cacheKey = taskId
    ? `${redisPrefix === undefined ? "*" : redisPrefix ?? ""}\0${workerId}\0${taskId}`
    : null;
  const cached = cacheKey ? taskStartCache.get(cacheKey) : undefined;
  if (cached) {
    if (await taskStartLocationStillValid(cached, taskId)) return cached;
    taskStartCache.delete(cacheKey!);
  }

  const streams = await findWorkerStreams(workerId, redisPrefix);
  if (streams.length === 0) return null;

  let best: TaskStartLocation | null = null;
  let inspectedStreams = 0;
  let firstStreamError: unknown = null;
  for (const stream of streams) {
    let entryId: string | null;
    try {
      entryId = await findTaskStartInStream(stream, taskId);
      inspectedStreams++;
    } catch (err) {
      firstStreamError ??= err;
      continue;
    }
    if (!entryId) continue;
    const location = { stream, entryId };
    if (!best || compareStreamIds(location.entryId, best.entryId) > 0) {
      best = location;
    }
  }

  if (firstStreamError) {
    if (inspectedStreams === 0) {
      throw firstStreamError;
    }
    if (!best) {
      throw new TaskOutputPartialReadError(
        "Task output lookup was incomplete because one or more worker output streams could not be read.",
      );
    }
  }

  if (best && cacheKey) {
    if (taskStartCache.size >= TASK_START_CACHE_MAX) {
      const firstKey = taskStartCache.keys().next().value;
      if (firstKey !== undefined) taskStartCache.delete(firstKey);
    }
    taskStartCache.set(cacheKey, best);
  }

  return best;
}

async function taskStartLocationStillValid(
  location: TaskStartLocation,
  taskId?: string,
): Promise<boolean> {
  try {
    const entries = await redis.xrange(location.stream, location.entryId, location.entryId, "COUNT", 1);
    if (!entries || entries.length === 0) return false;
    const fields = fieldsToMap(entries[0][1]);
    if (fields.type === "task_start" && (!taskId || fields.task_id === taskId)) {
      return true;
    }
    return Boolean(taskId && fields.task_id === taskId && fields.line !== undefined);
  } catch {
    return false;
  }
}

function fieldsToMap(fields: string[]): Record<string, string> {
  const fieldMap: Record<string, string> = {};
  for (let j = 0; j < fields.length; j += 2) {
    fieldMap[fields[j]] = fields[j + 1];
  }
  return fieldMap;
}

async function cursorEntryMatchesTask(stream: string, entryId: string, taskId: string): Promise<boolean> {
  const entries = await redis.xrange(stream, entryId, entryId, "COUNT", 1);
  if (!entries || entries.length === 0) return false;
  const fields = fieldsToMap(entries[0][1]);
  if (fields.type === "task_start") return fields.task_id === taskId;
  if (fields.task_id) return fields.task_id === taskId;
  return cursorFollowsTaskBoundary(stream, entryId, taskId);
}

async function cursorFollowsTaskBoundary(
  stream: string,
  entryId: string,
  taskId: string,
): Promise<boolean> {
  let endId = entryId;
  let scanned = 0;
  while (scanned < TASK_START_SCAN_MAX_ENTRIES) {
    const entries = await redis.xrevrange(
      stream,
      endId,
      "-",
      "COUNT",
      TASK_START_SCAN_BATCH_SIZE,
    );
    if (entries.length === 0) break;
    scanned += entries.length;

    for (const [candidateId, fields] of entries) {
      if (candidateId === entryId) continue;
      const fieldMap = fieldsToMap(fields);
      if (fieldMap.type === "task_start" && fieldMap.task_id) {
        return fieldMap.task_id === taskId;
      }
      if (fieldMap.type === "task_end" && fieldMap.task_id) return false;
      if (fieldMap.task_id) return fieldMap.task_id === taskId;
    }

    const oldestId = entries[entries.length - 1][0];
    endId = `(${oldestId}`;
  }
  return false;
}

async function findTaskStartInStream(
  stream: string,
  taskId?: string,
): Promise<string | null> {
  // Scan backward in chunks looking for task_start
  let endId = "+";
  let scanned = 0;
  let oldestTaggedTaskEntry: string | null = null;
  let latestTaggedTaskId: string | null = null;
  let oldestLatestTaggedTaskEntry: string | null = null;
  while (scanned < TASK_START_SCAN_MAX_ENTRIES) {
    const entries = await redis.xrevrange(
      stream,
      endId,
      "-",
      "COUNT",
      TASK_START_SCAN_BATCH_SIZE,
    );
    if (entries.length === 0) break;
    scanned += entries.length;

    for (const [entryId, fields] of entries) {
      const fieldMap: Record<string, string> = {};
      for (let j = 0; j < fields.length; j += 2) {
        fieldMap[fields[j]] = fields[j + 1];
      }

      if (fieldMap.type === "task_start" && (!taskId || fieldMap.task_id === taskId)) {
        if (!taskId && latestTaggedTaskId) {
          return fieldMap.task_id === latestTaggedTaskId
            ? entryId
            : oldestLatestTaggedTaskEntry;
        }
        return entryId;
      }
      if (taskId && fieldMap.task_id === taskId && fieldMap.line !== undefined) {
        oldestTaggedTaskEntry = entryId;
      } else if (!taskId && fieldMap.task_id && fieldMap.line !== undefined) {
        if (!latestTaggedTaskId) {
          latestTaggedTaskId = fieldMap.task_id;
          oldestLatestTaggedTaskEntry = entryId;
        } else if (fieldMap.task_id === latestTaggedTaskId) {
          oldestLatestTaggedTaskEntry = entryId;
        } else {
          return oldestLatestTaggedTaskEntry;
        }
      }
    }

    // Move cursor before the oldest entry in this batch using exclusive range syntax
    const oldestId = entries[entries.length - 1][0];
    endId = `(${oldestId}`;
  }
  return taskId ? oldestTaggedTaskEntry : oldestLatestTaggedTaskEntry;
}

export async function findTaskStartId(
  workerId: string,
  taskId?: string,
  redisPrefix?: RedisPrefixFilter,
): Promise<string | null> {
  const location = await findTaskStart(workerId, taskId, redisPrefix);
  return location?.entryId ?? null;
}

export async function findTaskOutputPrefix(
  workerId: string,
  taskId: string,
): Promise<string | null | undefined> {
  const location = await findTaskStart(workerId, taskId);
  return location ? outputStreamPrefix(location.stream) : undefined;
}

export async function findTaskOutputPrefixes(
  lookups: TaskOutputPrefixLookup[],
): Promise<TaskOutputPrefixLookupResult> {
  const normalizedLookups = new Map<string, TaskOutputPrefixLookup>();
  const tasksByWorker = new Map<string, Set<string>>();
  for (const lookup of lookups) {
    const workerId = lookup.workerId.trim();
    const taskId = lookup.taskId.trim();
    if (!workerId || !taskId || workerId === "(expired)") continue;
    const key = taskOutputPrefixLookupKey(workerId, taskId);
    if (normalizedLookups.has(key)) continue;
    normalizedLookups.set(key, { workerId, taskId });
    const cached = taskOutputPrefixCache.get(key);
    if (cached && Date.now() - cached.cachedAt < CACHE_TTL) continue;
    if (cached) taskOutputPrefixCache.delete(key);
    const tasks = tasksByWorker.get(workerId) || new Set<string>();
    tasks.add(taskId);
    tasksByWorker.set(workerId, tasks);
  }

  const prefixes = new Map<string, string | null | undefined>();
  if (normalizedLookups.size === 0) return { prefixes, degraded: false };

  for (const key of normalizedLookups.keys()) {
    const cached = taskOutputPrefixCache.get(key);
    prefixes.set(
      key,
      cached && Date.now() - cached.cachedAt < CACHE_TTL
        ? cached.prefix
        : undefined,
    );
  }
  if (tasksByWorker.size === 0) return { prefixes, degraded: false };

  let streams: string[];
  try {
    streams = await scanKeysMany(dashboardRedisKeyPatterns(["output:*"]));
  } catch {
    return { prefixes, degraded: true };
  }
  const streamsByWorker = new Map<string, string[]>();
  for (const stream of streams) {
    const workerId = workerIdFromOutputStream(stream);
    if (!workerId || !tasksByWorker.has(workerId)) continue;
    const workerStreams = streamsByWorker.get(workerId) || [];
    workerStreams.push(stream);
    streamsByWorker.set(workerId, workerStreams);
  }

  let degraded = false;
  for (const [workerId, taskIds] of tasksByWorker.entries()) {
    const workerStreams = streamsByWorker.get(workerId) || [];
    if (workerStreams.length === 0) continue;
    let inspectedStreams = 0;
    let firstStreamError: unknown = null;
    let workerDegraded = false;
    const bestByTask = new Map<string, TaskStartLocation>();

    for (const stream of workerStreams) {
      let starts: Map<string, string>;
      try {
        starts = await findTaskStartsInStream(stream, taskIds);
        inspectedStreams++;
      } catch (err) {
        firstStreamError ??= err;
        workerDegraded = true;
        degraded = true;
        continue;
      }
      for (const [taskId, entryId] of starts.entries()) {
        const current = bestByTask.get(taskId);
        if (!current || compareStreamIds(entryId, current.entryId) > 0) {
          bestByTask.set(taskId, { stream, entryId });
        }
      }
    }

    if (inspectedStreams === 0 && firstStreamError) {
      degraded = true;
      continue;
    }

    for (const [taskId, location] of bestByTask.entries()) {
      prefixes.set(
        taskOutputPrefixLookupKey(workerId, taskId),
        outputStreamPrefix(location.stream),
      );
    }
    if (!workerDegraded) {
      for (const [taskId, location] of bestByTask.entries()) {
        cacheTaskOutputPrefix(
          taskOutputPrefixLookupKey(workerId, taskId),
          outputStreamPrefix(location.stream),
        );
      }
    }
  }

  return { prefixes, degraded };
}

function cacheTaskOutputPrefix(
  key: string,
  prefix: string | null | undefined,
): void {
  if (prefix === undefined) return;
  if (taskOutputPrefixCache.size >= TASK_OUTPUT_PREFIX_CACHE_MAX) {
    const firstKey = taskOutputPrefixCache.keys().next().value;
    if (firstKey !== undefined) taskOutputPrefixCache.delete(firstKey);
  }
  taskOutputPrefixCache.set(key, { prefix, cachedAt: Date.now() });
}

async function findTaskStartsInStream(
  stream: string,
  taskIds: Set<string>,
): Promise<Map<string, string>> {
  const taskStarts = new Map<string, string>();
  const oldestTaggedTaskEntries = new Map<string, string>();
  let endId = "+";
  let scanned = 0;

  while (scanned < TASK_START_SCAN_MAX_ENTRIES) {
    const entries = await redis.xrevrange(
      stream,
      endId,
      "-",
      "COUNT",
      TASK_START_SCAN_BATCH_SIZE,
    );
    if (entries.length === 0) break;
    scanned += entries.length;

    for (const [entryId, fields] of entries) {
      const fieldMap = fieldsToMap(fields);
      const taskId = fieldMap.task_id;
      if (!taskId || !taskIds.has(taskId) || taskStarts.has(taskId)) continue;
      if (fieldMap.type === "task_start") {
        taskStarts.set(taskId, entryId);
      } else if (fieldMap.line !== undefined) {
        oldestTaggedTaskEntries.set(taskId, entryId);
      }
    }
    if (allTaskIdsHaveTaskEvidence(taskIds, taskStarts, oldestTaggedTaskEntries)) break;

    const oldestId = entries[entries.length - 1][0];
    endId = `(${oldestId}`;
  }

  return new Map([...oldestTaggedTaskEntries, ...taskStarts]);
}

function allTaskIdsHaveTaskEvidence(
  taskIds: Set<string>,
  taskStarts: Map<string, string>,
  taggedTaskEntries: Map<string, string>,
): boolean {
  for (const taskId of taskIds) {
    if (!taskStarts.has(taskId) && !taggedTaskEntries.has(taskId)) return false;
  }
  return true;
}

/**
 * Read task output starting from a given entry ID.
 * Stops if it encounters a task_end marker for the given taskId.
 * Returns formatted lines and whether the task is still in progress.
 */
type TaskOutputReadResult = {
  entries: Array<{ id: string; line: string }>;
  lastId: string;
  done: boolean;
  unavailable?: boolean;
};

export async function readTaskOutput(
  workerId: string,
  startId: string,
  lastId: string,
  taskId?: string,
  count = 100,
  redisPrefix?: RedisPrefixFilter,
): Promise<TaskOutputReadResult> {
  const stream = await findWorkerStream(workerId, redisPrefix);
  if (!stream) return { entries: [], lastId, done: false };
  return readTaskOutputFromStream(stream, startId, lastId, taskId, count);
}

export async function readTaskOutputFromStream(
  stream: string,
  startId: string,
  lastId: string,
  taskId?: string,
  count = 100,
): Promise<TaskOutputReadResult> {
  try {
    let cursorTrusted = true;
    if (taskId && lastId !== "0-0") {
      cursorTrusted = await cursorEntryMatchesTask(stream, lastId, taskId);
    }

    const effectiveLastId = taskId && lastId !== "0-0" && !cursorTrusted ? "0-0" : lastId;
    const fromId = effectiveLastId !== "0-0" ? `(${effectiveLastId}` : startId;
    const result = await redis.xrange(stream, fromId, "+", "COUNT", count);
    if (!result || result.length === 0) {
      if (taskId && effectiveLastId === "0-0") {
        return { entries: [], lastId, done: true, unavailable: true };
      }
      return { entries: [], lastId, done: false };
    }

    const entries: Array<{ id: string; line: string }> = [];
    let newLastId = effectiveLastId;
    let done = false;
    let unavailable = false;
    let matchedTaskStart = !taskId || (effectiveLastId !== "0-0" && cursorTrusted);
    let sawTrustedTaskEntry = matchedTaskStart;
    let appendBuffer = "";
    let appendBufferId = effectiveLastId;

    const flushAppendBuffer = () => {
      const line = appendBuffer.trimEnd();
      if (line.trim()) {
        entries.push({ id: appendBufferId, line });
      }
      appendBuffer = "";
    };

    for (const [entryId, fields] of result) {
      newLastId = entryId;
      const fieldMap = fieldsToMap(fields);
      const entryTaskId = fieldMap.task_id;

      if (taskId && entryTaskId && entryTaskId !== taskId) {
        if (!matchedTaskStart) {
          continue;
        }
        flushAppendBuffer();
        done = true;
        break;
      }

      // A cached task_start can become stale if Redis trims the stream. In that
      // case XRANGE begins at the retained minimum, which may be a regular line
      // from another task. Do not render anything until the exact task_start is
      // present in the returned window, or until a tagged output line proves it
      // belongs to the selected task.
      if (taskId && !matchedTaskStart) {
        if (fieldMap.type === "task_start") {
          if (fieldMap.task_id === taskId) {
            matchedTaskStart = true;
            sawTrustedTaskEntry = true;
          } else {
            continue;
          }
        } else if (entryTaskId === taskId) {
          matchedTaskStart = true;
          sawTrustedTaskEntry = true;
        } else {
          continue;
        }
      }

      // If an exact historical task has no task_end marker, stop before
      // displaying output from the next task on the same worker stream.
      if (taskId && fieldMap.type === "task_start" && fieldMap.task_id && fieldMap.task_id !== taskId) {
        flushAppendBuffer();
        done = true;
        break;
      }

      // Stop at task_end for this task
      if (fieldMap.type === "task_end" && (!taskId || fieldMap.task_id === taskId)) {
        flushAppendBuffer();
        const formatted = formatStreamEvent(fieldMap);
        if (formatted) entries.push({ id: entryId, line: formatted.line });
        done = true;
        break;
      }

      const formatted = formatStreamEvent(fieldMap);
      if (formatted) {
        if (formatted.append) {
          appendBuffer = limitFormattedJsonLine(appendBuffer + formatted.line);
          appendBufferId = entryId;
        } else {
          flushAppendBuffer();
          entries.push({ id: entryId, line: formatted.line });
        }
      }
    }
    flushAppendBuffer();

    if (taskId && !sawTrustedTaskEntry && result.length > 0) {
      return { entries: [], lastId: newLastId, done: true, unavailable: true };
    }

    return {
      entries,
      lastId: newLastId,
      done,
      ...(unavailable ? { unavailable: true } : {}),
    };
  } catch (err) {
    throw err;
  }
}

/**
 * Port of Python's format_stream_json_line(), plus raw terminal fallback for
 * interactive CLIs. Parses stream-json entries into readable output.
 */
export function formatStreamLine(fields: Record<string, string>): string | null {
  const formatted = formatStreamEvent(fields);
  return formatted?.line || null;
}

type FormattedStreamEvent = { line: string; append?: boolean };

function formatStreamEvent(fields: Record<string, string>): FormattedStreamEvent | null {
  // Task boundary markers (published by worker loop)
  if (fields.type === "task_start") {
    const resource = sanitizeLogMetadata(fields.resource, 120);
    const taskId = sanitizeLogMetadata(fields.task_id, 120);
    return { line: `${"─".repeat(3)} Task ${taskId}: ${resource} ${"─".repeat(40)}` };
  }
  if (fields.type === "task_end") {
    const status = sanitizeLogMetadata(fields.status, 80);
    const taskId = sanitizeLogMetadata(fields.task_id, 120);
    return { line: `${"─".repeat(3)} End ${taskId}: ${status} ${"─".repeat(42)}` };
  }

  // Regular output lines contain a "line" field. Non-interactive providers
  // usually emit JSON; interactive Claude emits raw terminal chunks.
  const line = fields.line;
  if (!line) return null;

  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(line.trim());
  } catch {
    const rawLine = formatRawTerminalLine(line);
    return rawLine ? { line: rawLine } : null;
  }

  if (typeof obj !== "object" || obj === null) return null;
  return formatJsonEvent(obj);
}

function formatJsonEvent(obj: Record<string, unknown>): FormattedStreamEvent | null {
  // Assistant messages with content blocks
  const msg = (obj.message as Record<string, unknown>) || obj;
  if (msg.role === "assistant" && typeof msg.content === "string") {
    const text = sanitizeTextFragment(msg.content);
    return text ? { line: text } : null;
  }

  if (msg.role === "assistant" && Array.isArray(msg.content)) {
    const parts: string[] = [];
    for (const block of msg.content) {
      if (typeof block !== "object" || block === null) continue;
      const b = block as Record<string, unknown>;
      const blockType = b.type;

      if (blockType === "text") {
        const text = typeof b.text === "string" ? sanitizeTextFragment(b.text) : "";
        if (text) parts.push(text);
      } else if (blockType === "tool_use") {
        const name = sanitizeLogMetadata(b.name, 80);
        const inp = typeof b.input === "object" && b.input !== null
          ? (b.input as Record<string, unknown>)
          : {};

        if (name === "Bash") {
          const cmd = sanitizeLogMetadata(inp.command, 120);
          parts.push(`  $ ${cmd}`);
        } else if (["Read", "Edit", "Write"].includes(name)) {
          parts.push(`  ${name} ${sanitizeLogMetadata(inp.file_path, 240)}`);
        } else if (name === "Glob") {
          parts.push(`  Glob ${sanitizeLogMetadata(inp.pattern, 240)}`);
        } else if (name === "Grep") {
          parts.push(`  Grep ${sanitizeLogMetadata(inp.pattern, 240)}`);
        } else {
          parts.push(`  ${name}`);
        }
      }
    }

    return parts.length > 0 ? { line: limitFormattedJsonLine(parts.join("\n")) } : null;
  }

  const eventType = obj.type;
  if (eventType === "text" && typeof obj.data === "string") {
    const text = sanitizeTextFragment(obj.data);
    return text ? { line: text, append: true } : null;
  }

  if (eventType === "error" && typeof obj.message === "string") {
    const text = formatRawTerminalLine(obj.message);
    return text ? { line: `Error: ${text}` } : null;
  }

  if (eventType === "turn.failed") {
    const error = obj.error;
    const message =
      typeof error === "object" && error !== null
        ? (error as Record<string, unknown>).message
        : null;
    if (typeof message === "string") {
      const text = formatRawTerminalLine(message);
      return text ? { line: `Error: ${text}` } : null;
    }
  }

  if (eventType === "item.completed" || eventType === "item.started") {
    const item = obj.item;
    if (typeof item !== "object" || item === null) return null;
    const formatted = formatCodexItem(item as Record<string, unknown>, eventType);
    return formatted ? { line: formatted } : null;
  }

  return null;
}

function formatCodexItem(item: Record<string, unknown>, eventType: unknown): string | null {
  const itemType = item.type;
  const status = typeof item.status === "string" ? ` ${sanitizeLogMetadata(item.status, 40)}` : "";

  if (itemType === "agent_message" && typeof item.text === "string") {
    return sanitizeTextFragment(item.text) || null;
  }
  if (itemType === "file_change" && Array.isArray(item.changes)) {
    const changes = item.changes
      .map((change) => {
        if (typeof change !== "object" || change === null) return "";
        const c = change as Record<string, unknown>;
        const kind = sanitizeLogMetadata(c.kind || "change", 40);
        const path = sanitizeLogMetadata(c.path, 240);
        return `${kind} ${path}`;
      })
      .filter(Boolean);
    return changes.length > 0 ? `  File change${status}: ${changes.join(", ")}` : null;
  }
  if (itemType === "command_execution") {
    const command = sanitizeLogMetadata(item.command || item.cmd || item.id, 240);
    return `  Command ${eventType === "item.started" ? "started" : "completed"}${status}: ${command}`;
  }
  return null;
}

export function formatRawTerminalLine(line: string): string | null {
  const stripped = line
    .replace(ANSI_RE, "")
    .replace(/\r\n?/g, "\n")
    .replace(CONTROL_RE, "");
  const cleaned = stripped
    .split("\n")
    .map((part) => part.replace(/[ \t]+$/g, ""))
    .map((part) => part.replace(/[ \t]{3,}/g, "  ").trim())
    .filter((part) => part && !BOX_DRAWING_ONLY_RE.test(part));
  const deduped = cleaned.filter((part, index) => index === 0 || part !== cleaned[index - 1]);
  const visible = deduped.join("\n");
  return visible ? limitFormattedJsonLine(visible) : null;
}

function sanitizeTextFragment(text: string): string {
  return limitFormattedJsonLine(text
    .replace(ANSI_RE, "")
    .replace(CONTROL_RE, "")
    .replace(/\r\n?/g, "\n"));
}

function limitFormattedJsonLine(text: string): string {
  if (text.length <= FORMATTED_JSON_LINE_MAX_CHARS) return text;
  const prefixLength = Math.max(0, FORMATTED_JSON_LINE_MAX_CHARS - TRUNCATED_SUFFIX.length);
  return `${text.slice(0, prefixLength)}${TRUNCATED_SUFFIX}`;
}

function sanitizeLogMetadata(value: unknown, maxLength: number): string {
  const raw = value === undefined || value === null || value === "" ? "?" : String(value);
  const formatted = formatRawTerminalLine(raw);
  return (formatted || "?").replace(/\n+/g, " ").slice(0, maxLength);
}

function compareStreamIds(a: string, b: string): number {
  const [aMs, aSeq] = streamIdParts(a) ?? [0, 0];
  const [bMs, bSeq] = streamIdParts(b) ?? [0, 0];
  return aMs - bMs || aSeq - bSeq;
}

function streamIdParts(entryId: string): [number, number] | null {
  const match = entryId.match(/^(\d+)-(\d+)$/);
  if (!match) return null;
  const ms = Number(match[1]);
  const seq = Number(match[2]);
  return Number.isSafeInteger(ms) && Number.isSafeInteger(seq) ? [ms, seq] : null;
}
