import { redis, scanKeys } from "./redis.js";
import type { SystemSnapshot, StuckTask } from "./types.js";

const LOCK_TTL = parseInt(process.env.LOCK_TTL_SECONDS || "180", 10); // seconds — must match Python LOCK_TTL
const MAX_PER_SHA_ATTEMPTS = 3;

export async function detectStuck(snapshot: SystemSnapshot): Promise<StuckTask[]> {
  const stuck: StuckTask[] = [];

  // 1. Orphaned pending: pending marker exists but no corresponding lock
  const pendingKeys = await scanKeys("*:pending:*");

  // Parse all pending keys upfront so we can batch Redis calls
  type PendingEntry = { key: string; lockKey: string; resourceType: string; resourceId: string };
  const pendingEntries: PendingEntry[] = [];
  for (const key of pendingKeys) {
    // key format: prefix:pending:resource_type:repo:resource_id
    const afterPending = key.replace(/^[^:]+:pending:/, "");
    const parts = afterPending.split(":");
    if (parts.length < 3) continue;
    const resourceType = parts[0]; // "pr" or "issue"
    const resourceId = parts[parts.length - 1];
    const repo = parts.slice(1, -1).join(":");
    const prefix = key.split(":")[0];
    const lockKey = `${prefix}:lock:${resourceType}:${repo}:${resourceId}`;
    pendingEntries.push({ key, lockKey, resourceType, resourceId });
  }

  if (pendingEntries.length > 0) {
    // Batch all EXISTS + TTL calls into a single pipeline
    const lockPipeline = redis.pipeline();
    for (const { lockKey, key } of pendingEntries) {
      lockPipeline.exists(lockKey);
      lockPipeline.ttl(key);
    }
    const results = await lockPipeline.exec();

    for (let i = 0; i < pendingEntries.length; i++) {
      const { resourceType, resourceId } = pendingEntries[i];
      const lockExists = (results?.[i * 2]?.[1] as number) ?? 0;
      if (!lockExists) {
        const ttl = (results?.[i * 2 + 1]?.[1] as number) ?? -1;
        if (ttl === -1 || (ttl > 0 && ttl < 1200)) {  // 1200s = 20 min — maximum expected pending lifetime
          stuck.push({
            resource_type: resourceType,
            resource_id: resourceId,
            reason: ttl === -1
              ? "Queued with no TTL — pending marker will never expire"
              : `Queued but no worker has picked it up (pending TTL: ${ttl}s)`,
            severity: ttl === -1 || ttl < 600 ? "critical" : "warning",
          });
        }
      }
    }
  }

  // 2. High attempt counts
  // Label format: "owner/repo PR #N" — extract just the PR number for resource_id
  for (const [label, count] of Object.entries(snapshot.attempt_counts)) {
    const prMatch = label.match(/PR #(\d+)$/);
    const resourceId = prMatch ? prMatch[1] : label;
    if (count >= MAX_PER_SHA_ATTEMPTS) {
      stuck.push({
        resource_type: "pr",
        resource_id: resourceId,
        reason: `Attempt count at max (${count}/${MAX_PER_SHA_ATTEMPTS})`,
        severity: "critical",
      });
    } else if (count >= MAX_PER_SHA_ATTEMPTS - 1) {
      stuck.push({
        resource_type: "pr",
        resource_id: resourceId,
        reason: `Attempt count near max (${count}/${MAX_PER_SHA_ATTEMPTS})`,
        severity: "warning",
      });
    }
  }

  // 3. Stale consumer group entries (pending entries with high idle time)
  for (const group of snapshot.consumer_groups) {
    if (group.pending === 0) continue;
    try {
      const details = await redis.call(
        "XPENDING", group.stream, group.name, "-", "+", "10"
      ) as unknown[][];

      for (const entry of details) {
        if (!Array.isArray(entry) || entry.length < 4) continue;
        const idleMs = entry[2] as number;
        const deliveryCount = entry[3] as number;

        if (idleMs > LOCK_TTL * 1000) {
          stuck.push({
            resource_type: "stream",
            resource_id: `${group.stream}/${group.name}`,
            reason: `Pending entry idle for ${Math.round(idleMs / 1000)}s (${deliveryCount} deliveries)`,
            severity: deliveryCount >= 2 ? "critical" : "warning",
          });
        }
      }
    } catch {
      // ignore XPENDING errors
    }
  }

  return stuck;
}
