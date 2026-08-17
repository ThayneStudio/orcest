import { Redis } from "ioredis";

let redisClient: Redis | null = null;

const UNPREFIXED_ALLOWLIST_VALUES = new Set(["unprefixed", "none"]);

export function redisPortFromEnv(raw = process.env.REDIS_PORT): number {
  const trimmed = raw?.trim() ?? "";
  if (!trimmed || !/^\d+$/.test(trimmed)) return 6379;
  const parsed = Number(trimmed);
  return Number.isSafeInteger(parsed) && parsed >= 1 && parsed <= 65535 ? parsed : 6379;
}

function createRedisClient(): Redis {
  return new Redis({
    host: process.env.REDIS_HOST || "localhost",
    port: redisPortFromEnv(),
    password: process.env.REDIS_PASSWORD || process.env.ORCEST_REDIS_PASSWORD || undefined,
    maxRetriesPerRequest: 3,
    retryStrategy(times: number) {
      return Math.min(times * 200, 5000);
    },
  });
}

export function getRedis(): Redis {
  if (!redisClient) {
    redisClient = createRedisClient();
  }
  return redisClient;
}

export async function quitRedis(): Promise<void> {
  const client = redisClient;
  if (client) {
    await client.quit();
    if (redisClient === client) redisClient = null;
  }
}

export const redis = new Proxy({} as Redis, {
  get(_target, property) {
    const client = getRedis();
    const value = (client as unknown as Record<PropertyKey, unknown>)[property];
    return typeof value === "function" ? value.bind(client) : value;
  },
  set(_target, property, value) {
    const client = getRedis() as unknown as Record<PropertyKey, unknown>;
    client[property] = value;
    return true;
  },
});

export async function healthCheck(): Promise<boolean> {
  try {
    const result = await redis.ping();
    return result === "PONG";
  } catch {
    return false;
  }
}

// Every pattern is a full server-side keyspace walk, and a snapshot build issues
// more than twenty of them per poll. COUNT 100 turned that into thousands of
// sequential round-trips per 2s refresh interval.
export const SCAN_COUNT = 1000;

/**
 * Collect all keys matching a pattern via SCAN (non-blocking iteration).
 */
export async function scanKeys(pattern: string): Promise<string[]> {
  const keys: string[] = [];
  let cursor = "0";
  do {
    const [nextCursor, batch] = await redis.scan(cursor, "MATCH", pattern, "COUNT", SCAN_COUNT);
    cursor = nextCursor;
    keys.push(...batch);
  } while (cursor !== "0");
  return keys;
}

/**
 * Scan several patterns at once. The passes run concurrently — one sequential
 * keyspace walk per pattern cannot keep up with the snapshot refresh interval.
 */
export async function scanKeysMany(patterns: string[]): Promise<string[]> {
  const uniquePatterns = [...new Set(patterns)];
  const batches = await Promise.all(uniquePatterns.map((pattern) => scanKeys(pattern)));
  const keys = new Set<string>();
  for (const batch of batches) {
    for (const key of batch) keys.add(key);
  }
  return [...keys].sort();
}

export function dashboardRedisPrefixes(
  raw = process.env.DASHBOARD_REDIS_PREFIXES,
): string[] | null {
  if (raw === undefined) return null;

  const prefixes: string[] = [];
  const seen = new Set<string>();
  for (const part of raw.split(",")) {
    const value = part.trim();
    if (!value) continue;
    const normalized = UNPREFIXED_ALLOWLIST_VALUES.has(value.toLowerCase()) ? "" : value;
    if (seen.has(normalized)) continue;
    seen.add(normalized);
    prefixes.push(normalized);
  }

  if (prefixes.length === 0) {
    throw new Error(
      "DASHBOARD_REDIS_PREFIXES must contain at least one prefix or the 'unprefixed' value",
    );
  }
  return prefixes;
}

export function assertValidDashboardRedisPrefixes(): void {
  dashboardRedisPrefixes();
}

function escapeRedisGlob(value: string): string {
  return value.replace(/[\\*?\[\]]/g, "\\$&");
}

export function dashboardRedisKeyPatterns(suffixes: string[]): string[] {
  const configuredPrefixes = dashboardRedisPrefixes();
  const patterns = new Set<string>();

  if (!configuredPrefixes) {
    for (const suffix of suffixes) {
      patterns.add(`*:${suffix}`);
      patterns.add(suffix);
    }
    return [...patterns];
  }

  for (const prefix of configuredPrefixes) {
    for (const suffix of suffixes) {
      patterns.add(prefix ? `${escapeRedisGlob(prefix)}:${suffix}` : suffix);
    }
  }
  return [...patterns];
}
