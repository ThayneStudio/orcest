import { describe, expect, it } from "vitest";
import { readDashboardPolicy } from "./policy.js";

describe("readDashboardPolicy", () => {
  it("uses Python-compatible defaults", () => {
    expect(readDashboardPolicy({})).toEqual({
      max_attempts: 3,
      pending_task_ttl_seconds: 16500,
      lock_ttl_seconds: 180,
    });
  });

  it("uses explicit dashboard and orcest env overrides", () => {
    expect(readDashboardPolicy({
      ORCEST_MAX_ATTEMPTS: "5",
      ORCEST_RUNNER_TIMEOUT_SECONDS: "1200",
      ORCEST_RUNNER_MAX_RETRIES: "2",
      LOCK_TTL_SECONDS: "240",
    })).toEqual({
      max_attempts: 5,
      pending_task_ttl_seconds: 2700,
      lock_ttl_seconds: 240,
    });
  });

  it("prefers dashboard and orcest lock TTL env over legacy lock TTL", () => {
    expect(readDashboardPolicy({
      LOCK_TTL_SECONDS: "180",
      ORCEST_LOCK_TTL_SECONDS: "240",
    }).lock_ttl_seconds).toBe(240);
    expect(readDashboardPolicy({
      LOCK_TTL_SECONDS: "180",
      ORCEST_LOCK_TTL_SECONDS: "240",
      DASHBOARD_LOCK_TTL_SECONDS: "360",
    }).lock_ttl_seconds).toBe(360);
  });

  it("lets explicit pending TTL override computed runner TTL", () => {
    expect(readDashboardPolicy({
      ORCEST_RUNNER_TIMEOUT_SECONDS: "1200",
      ORCEST_RUNNER_MAX_RETRIES: "2",
      ORCEST_PENDING_TASK_TTL_SECONDS: "999",
    }).pending_task_ttl_seconds).toBe(999);
  });

  it("ignores malformed integer env values", () => {
    expect(readDashboardPolicy({
      ORCEST_MAX_ATTEMPTS: "3x",
      ORCEST_RUNNER_TIMEOUT_SECONDS: "1800s",
      ORCEST_RUNNER_MAX_RETRIES: "2.5",
      LOCK_TTL_SECONDS: " 0 ",
    })).toEqual({
      max_attempts: 3,
      pending_task_ttl_seconds: 16500,
      lock_ttl_seconds: 180,
    });
  });
});
