import { afterEach, describe, expect, it, vi } from "vitest";

const redisMock = vi.hoisted(() => {
  const quit = vi.fn();
  const ping = vi.fn();
  const Redis = vi.fn(function RedisMock() {
    return { ping, quit };
  });
  return { Redis, ping, quit };
});

const redisCtor = redisMock.Redis;
const quit = redisMock.quit;
const ping = redisMock.ping;
const originalDashboardRedisPrefixes = process.env.DASHBOARD_REDIS_PREFIXES;
const originalRedisPort = process.env.REDIS_PORT;

vi.mock("ioredis", () => ({
  Redis: redisCtor,
}));

async function importRedisModule(): Promise<typeof import("./redis.js")> {
  vi.resetModules();
  redisCtor.mockReset();
  quit.mockReset();
  ping.mockReset();
  redisCtor.mockImplementation(function RedisMock() {
    return {
      ping,
      quit,
    };
  });
  return import("./redis.js");
}

afterEach(() => {
  if (originalDashboardRedisPrefixes === undefined) delete process.env.DASHBOARD_REDIS_PREFIXES;
  else process.env.DASHBOARD_REDIS_PREFIXES = originalDashboardRedisPrefixes;
  if (originalRedisPort === undefined) delete process.env.REDIS_PORT;
  else process.env.REDIS_PORT = originalRedisPort;
  vi.restoreAllMocks();
});

describe("redis client lifecycle", () => {
  it("does not create a Redis connection at import time", async () => {
    await importRedisModule();

    expect(redisCtor).not.toHaveBeenCalled();
  });

  it("creates the Redis client lazily on first command", async () => {
    const { redis } = await importRedisModule();
    ping.mockResolvedValue("PONG");

    await redis.ping();

    expect(redisCtor).toHaveBeenCalledTimes(1);
    expect(ping).toHaveBeenCalledTimes(1);
  });

  it("does not create a Redis connection just to quit an unused client", async () => {
    const { quitRedis } = await importRedisModule();

    await quitRedis();

    expect(redisCtor).not.toHaveBeenCalled();
    expect(quit).not.toHaveBeenCalled();
  });

  it("quits an initialized Redis client once", async () => {
    const { redis, quitRedis } = await importRedisModule();
    ping.mockResolvedValue("PONG");
    quit.mockResolvedValue("OK");

    await redis.ping();
    await quitRedis();
    await quitRedis();

    expect(redisCtor).toHaveBeenCalledTimes(1);
    expect(quit).toHaveBeenCalledTimes(1);
  });

  it("retains the initialized Redis client when quit fails so cleanup can retry", async () => {
    const { redis, quitRedis } = await importRedisModule();
    ping.mockResolvedValue("PONG");
    quit.mockRejectedValueOnce(new Error("quit failed")).mockResolvedValueOnce("OK");

    await redis.ping();
    await expect(quitRedis()).rejects.toThrow("quit failed");
    await quitRedis();

    expect(redisCtor).toHaveBeenCalledTimes(1);
    expect(quit).toHaveBeenCalledTimes(2);
  });
});

describe("redisPortFromEnv", () => {
  it("accepts valid Redis ports", async () => {
    const { redisPortFromEnv } = await importRedisModule();

    expect(redisPortFromEnv("6379")).toBe(6379);
    expect(redisPortFromEnv(" 6380 ")).toBe(6380);
  });

  it("falls back for malformed, zero, or out-of-range Redis ports", async () => {
    const { redisPortFromEnv } = await importRedisModule();

    expect(redisPortFromEnv(undefined)).toBe(6379);
    expect(redisPortFromEnv("6379abc")).toBe(6379);
    expect(redisPortFromEnv("1.5")).toBe(6379);
    expect(redisPortFromEnv("0")).toBe(6379);
    expect(redisPortFromEnv("65536")).toBe(6379);
  });
});

describe("dashboard Redis key patterns", () => {
  it("keeps unscoped wildcard behavior when no prefix allowlist is configured", async () => {
    delete process.env.DASHBOARD_REDIS_PREFIXES;
    const { dashboardRedisKeyPatterns, dashboardRedisPrefixes } = await importRedisModule();

    expect(dashboardRedisPrefixes()).toBeNull();
    expect(dashboardRedisKeyPatterns(["tasks:*", "results"])).toEqual([
      "*:tasks:*",
      "tasks:*",
      "*:results",
      "results",
    ]);
  });

  it("limits scans to configured prefixes and optional unprefixed keys", async () => {
    process.env.DASHBOARD_REDIS_PREFIXES = "orcest, project-a, unprefixed, orcest,";
    const { dashboardRedisKeyPatterns, dashboardRedisPrefixes } = await importRedisModule();

    expect(dashboardRedisPrefixes()).toEqual(["orcest", "project-a", ""]);
    expect(dashboardRedisKeyPatterns(["tasks:*"])).toEqual([
      "orcest:tasks:*",
      "project-a:tasks:*",
      "tasks:*",
    ]);
  });

  it("does not include unprefixed keys for blank allowlist entries", async () => {
    process.env.DASHBOARD_REDIS_PREFIXES = "orcest,";
    const { dashboardRedisKeyPatterns, dashboardRedisPrefixes } = await importRedisModule();

    expect(dashboardRedisPrefixes()).toEqual(["orcest"]);
    expect(dashboardRedisKeyPatterns(["tasks:*"])).toEqual(["orcest:tasks:*"]);
  });

  it.each(["", "   ", ",", " , , "])(
    "rejects a configured allowlist with no usable entries (%j)",
    async (configured) => {
      process.env.DASHBOARD_REDIS_PREFIXES = configured;
      const { dashboardRedisKeyPatterns, dashboardRedisPrefixes } =
        await importRedisModule();

      expect(() => dashboardRedisPrefixes()).toThrow(/must contain at least one prefix/);
      expect(() => dashboardRedisKeyPatterns(["tasks:*"])).toThrow(
        /must contain at least one prefix/,
      );
    },
  );

  it("escapes Redis glob metacharacters in configured prefixes", async () => {
    process.env.DASHBOARD_REDIS_PREFIXES = "project[1], literal*";
    const { dashboardRedisKeyPatterns } = await importRedisModule();

    expect(dashboardRedisKeyPatterns(["results"])).toEqual([
      "project\\[1\\]:results",
      "literal\\*:results",
    ]);
  });
});
