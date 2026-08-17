export interface DashboardPolicy {
  max_attempts: number;
  pending_task_ttl_seconds: number;
  lock_ttl_seconds: number;
}

const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_RUNNER_TIMEOUT_SECONDS = 5400;
const DEFAULT_RUNNER_MAX_RETRIES = 3;
const DEFAULT_PENDING_TTL_BUFFER_SECONDS = 300;
const DEFAULT_LOCK_TTL_SECONDS = 180;

type Env = Record<string, string | undefined>;

function positiveInt(value: string | undefined): number | null {
  if (value === undefined || value.trim() === "") return null;
  const trimmed = value.trim();
  if (!/^[1-9]\d*$/.test(trimmed)) return null;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function firstPositiveInt(env: Env, names: string[], fallback: number): number {
  for (const name of names) {
    const parsed = positiveInt(env[name]);
    if (parsed !== null) return parsed;
  }
  return fallback;
}

export function readDashboardPolicy(env: Env = process.env): DashboardPolicy {
  const runnerTimeout = firstPositiveInt(
    env,
    ["DASHBOARD_RUNNER_TIMEOUT_SECONDS", "ORCEST_RUNNER_TIMEOUT_SECONDS"],
    DEFAULT_RUNNER_TIMEOUT_SECONDS,
  );
  const runnerMaxRetries = firstPositiveInt(
    env,
    ["DASHBOARD_RUNNER_MAX_RETRIES", "ORCEST_RUNNER_MAX_RETRIES"],
    DEFAULT_RUNNER_MAX_RETRIES,
  );
  const computedPendingTtl =
    runnerTimeout * runnerMaxRetries + DEFAULT_PENDING_TTL_BUFFER_SECONDS;

  return {
    max_attempts: firstPositiveInt(
      env,
      ["DASHBOARD_MAX_ATTEMPTS", "ORCEST_MAX_ATTEMPTS"],
      DEFAULT_MAX_ATTEMPTS,
    ),
    pending_task_ttl_seconds: firstPositiveInt(
      env,
      ["DASHBOARD_PENDING_TASK_TTL_SECONDS", "ORCEST_PENDING_TASK_TTL_SECONDS"],
      computedPendingTtl,
    ),
    lock_ttl_seconds: firstPositiveInt(
      env,
      ["DASHBOARD_LOCK_TTL_SECONDS", "ORCEST_LOCK_TTL_SECONDS", "LOCK_TTL_SECONDS"],
      DEFAULT_LOCK_TTL_SECONDS,
    ),
  };
}
