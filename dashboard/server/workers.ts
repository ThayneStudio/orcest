import { redis, scanKeys } from "./redis.js";

/**
 * Return worker IDs that have output streams with recent activity.
 * Also checks for workers with active locks.
 */
export async function discoverWorkers(): Promise<string[]> {
  try {
    const streams = await scanKeys("*:output:*");
    const workers: Array<{ id: string; lastEntryMs: number }> = [];

    for (const stream of streams) {
      const workerId = stream.replace(/^[^:]+:output:/, "");
      // Check the last entry timestamp to determine recency
      try {
        const entries = await redis.xrevrange(stream, "+", "-", "COUNT", 1);
        if (entries.length > 0) {
          const entryId = entries[0][0];
          const ms = parseInt(entryId.split("-")[0], 10);
          workers.push({ id: workerId, lastEntryMs: ms });
        }
      } catch {
        // stream might be empty or gone
      }
    }

    // Sort by most recent activity first
    workers.sort((a, b) => b.lastEntryMs - a.lastEntryMs);

    // Only include workers with activity in the last 7 days
    const cutoff = Date.now() - 7 * 24 * 60 * 60 * 1000;
    return workers.filter((w) => w.lastEntryMs > cutoff).map((w) => w.id);
  } catch {
    return [];
  }
}

/**
 * Find the prefixed stream key for a worker (e.g., orcest:output:worker-1).
 */
// Cache resolved stream keys — worker stream names don't change during uptime
const streamCache = new Map<string, string>();

async function findWorkerStream(workerId: string): Promise<string | null> {
  const cached = streamCache.get(workerId);
  if (cached) return cached;
  try {
    const matches = await scanKeys(`*:output:${workerId}`);
    if (matches.length > 0) {
      streamCache.set(workerId, matches[0]);
      return matches[0];
    }
    return null;
  } catch {
    return null;
  }
}

/**
 * Get a starting position near the tail of a worker's stream.
 * Returns the entry ID to start reading from (the last 200 entries).
 */
export async function getStreamTailId(
  workerId: string,
): Promise<string> {
  const stream = await findWorkerStream(workerId);
  if (!stream) return "0-0";
  try {
    // Read the last 200 entries in reverse, take the oldest one's ID as our start
    const entries = await redis.xrevrange(stream, "+", "-", "COUNT", 200);
    if (entries.length === 0) return "0-0";
    // The last entry in xrevrange (oldest of the batch) — start just before it
    const oldestId = entries[entries.length - 1][0];
    return oldestId;
  } catch {
    return "0-0";
  }
}

/**
 * Non-blocking read of worker output using XRANGE.
 */
export async function readWorkerOutputNonBlocking(
  workerId: string,
  lastId: string,
  count = 50
): Promise<{ entries: Array<{ id: string; line: string }>; lastId: string }> {
  const stream = await findWorkerStream(workerId);
  if (!stream) return { entries: [], lastId };
  try {
    const startId = lastId === "0-0" ? "-" : `(${lastId}`;
    const result = await redis.xrange(stream, startId, "+", "COUNT", count);
    if (!result || result.length === 0) return { entries: [], lastId };

    const entries: Array<{ id: string; line: string }> = [];
    let newLastId = lastId;

    for (const [entryId, fields] of result) {
      newLastId = entryId;
      const fieldMap: Record<string, string> = {};
      for (let i = 0; i < fields.length; i += 2) {
        fieldMap[fields[i]] = fields[i + 1];
      }

      const formatted = formatStreamLine(fieldMap);
      if (formatted) {
        entries.push({ id: entryId, line: formatted });
      }
    }

    return { entries, lastId: newLastId };
  } catch {
    return { entries: [], lastId };
  }
}

/**
 * Find the stream entry ID where a task started on a given worker.
 * Scans backward from the end of the stream looking for a task_start marker.
 * If taskId is provided, matches that specific task; otherwise finds the most recent task.
 */
export async function findTaskStartId(
  workerId: string,
  taskId?: string,
): Promise<string | null> {
  const stream = await findWorkerStream(workerId);
  if (!stream) return null;

  try {
    // Scan backward in chunks looking for task_start
    let endId = "+";
    for (let i = 0; i < 20; i++) {  // Max 20 chunks of 200 = 4000 entries
      const entries = await redis.xrevrange(stream, endId, "-", "COUNT", 200);
      if (entries.length === 0) break;

      for (const [entryId, fields] of entries) {
        const fieldMap: Record<string, string> = {};
        for (let j = 0; j < fields.length; j += 2) {
          fieldMap[fields[j]] = fields[j + 1];
        }

        if (fieldMap.type === "task_start") {
          if (!taskId || fieldMap.task_id === taskId) {
            return entryId;
          }
        }
      }

      // Move cursor before the oldest entry in this batch
      const oldestId = entries[entries.length - 1][0];
      // Decrement the ID to exclude it from next read
      const [ms, seq] = oldestId.split("-");
      const newSeq = parseInt(seq, 10) - 1;
      endId = newSeq >= 0 ? `${ms}-${newSeq}` : `${parseInt(ms, 10) - 1}-99999`;
    }
  } catch {
    // ignore
  }
  return null;
}

/**
 * Read task output starting from a given entry ID.
 * Stops if it encounters a task_end marker for the given taskId.
 * Returns formatted lines and whether the task is still in progress.
 */
export async function readTaskOutput(
  workerId: string,
  startId: string,
  lastId: string,
  taskId?: string,
  count = 100,
): Promise<{ entries: Array<{ id: string; line: string }>; lastId: string; done: boolean }> {
  const stream = await findWorkerStream(workerId);
  if (!stream) return { entries: [], lastId, done: false };

  try {
    const fromId = lastId !== "0-0" ? `(${lastId}` : startId;
    const result = await redis.xrange(stream, fromId, "+", "COUNT", count);
    if (!result || result.length === 0) return { entries: [], lastId, done: false };

    const entries: Array<{ id: string; line: string }> = [];
    let newLastId = lastId;
    let done = false;

    for (const [entryId, fields] of result) {
      newLastId = entryId;
      const fieldMap: Record<string, string> = {};
      for (let j = 0; j < fields.length; j += 2) {
        fieldMap[fields[j]] = fields[j + 1];
      }

      // Stop at task_end for this task
      if (fieldMap.type === "task_end" && (!taskId || fieldMap.task_id === taskId)) {
        const formatted = formatStreamLine(fieldMap);
        if (formatted) entries.push({ id: entryId, line: formatted });
        done = true;
        break;
      }

      const formatted = formatStreamLine(fieldMap);
      if (formatted) {
        entries.push({ id: entryId, line: formatted });
      }
    }

    return { entries, lastId: newLastId, done };
  } catch {
    return { entries: [], lastId, done: false };
  }
}

/**
 * Port of Python's format_stream_json_line().
 * Parses Claude stream-json entries into readable output.
 */
function formatStreamLine(fields: Record<string, string>): string | null {
  // Task boundary markers (published by worker loop)
  if (fields.type === "task_start") {
    const resource = fields.resource || "?";
    const taskId = fields.task_id || "?";
    return `${"─".repeat(3)} Task ${taskId}: ${resource} ${"─".repeat(40)}`;
  }
  if (fields.type === "task_end") {
    const status = fields.status || "?";
    const taskId = fields.task_id || "?";
    return `${"─".repeat(3)} End ${taskId}: ${status} ${"─".repeat(42)}`;
  }

  // Regular output lines contain a "line" field with JSON
  const line = fields.line;
  if (!line) return null;

  let obj: Record<string, unknown>;
  try {
    obj = JSON.parse(line.trim());
  } catch {
    return null;
  }

  if (typeof obj !== "object" || obj === null) return null;

  // Assistant messages with content blocks
  const msg = (obj.message as Record<string, unknown>) || obj;
  if (msg.role !== "assistant" || !Array.isArray(msg.content)) return null;

  const parts: string[] = [];
  for (const block of msg.content) {
    if (typeof block !== "object" || block === null) continue;
    const b = block as Record<string, unknown>;
    const blockType = b.type;

    if (blockType === "text") {
      const text = b.text as string;
      if (text) parts.push(text);
    } else if (blockType === "tool_use") {
      const name = String(b.name || "?");
      const inp = (b.input as Record<string, unknown>) || {};

      if (name === "Bash") {
        const cmd = String(inp.command || "?").slice(0, 120);
        parts.push(`  $ ${cmd}`);
      } else if (["Read", "Edit", "Write"].includes(name)) {
        parts.push(`  ${name} ${String(inp.file_path || "?")}`);
      } else if (name === "Glob") {
        parts.push(`  Glob ${String(inp.pattern || "?")}`);
      } else if (name === "Grep") {
        parts.push(`  Grep ${String(inp.pattern || "?")}`);
      } else {
        parts.push(`  ${name}`);
      }
    }
  }

  return parts.length > 0 ? parts.join("\n") : null;
}
