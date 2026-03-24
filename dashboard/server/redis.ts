import { Redis } from "ioredis";

const REDIS_HOST = process.env.REDIS_HOST || "localhost";
const REDIS_PORT = parseInt(process.env.REDIS_PORT || "6379", 10);

export const redis = new Redis({
  host: REDIS_HOST,
  port: REDIS_PORT,
  maxRetriesPerRequest: 3,
  retryStrategy(times: number) {
    return Math.min(times * 200, 5000);
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

/**
 * Collect all keys matching a pattern via SCAN (non-blocking iteration).
 */
export async function scanKeys(pattern: string): Promise<string[]> {
  const keys: string[] = [];
  let cursor = "0";
  do {
    const [nextCursor, batch] = await redis.scan(cursor, "MATCH", pattern, "COUNT", 100);
    cursor = nextCursor;
    keys.push(...batch);
  } while (cursor !== "0");
  return keys;
}
