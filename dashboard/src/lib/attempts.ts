export type AttemptSeverity = "normal" | "warning" | "critical";

export interface AttemptCountRow {
  label: string;
  count: number;
  severity: AttemptSeverity;
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

export function normalizeAttemptCounts(
  attempts: Record<string, unknown>,
  maxAttempts = 3,
): AttemptCountRow[] {
  const countsByLabel = new Map<string, number>();
  for (const [rawLabel, rawCount] of Object.entries(attempts)) {
    const label = rawLabel.trim();
    const parsed = numericValue(rawCount);
    if (parsed === null) continue;
    if (!Number.isInteger(parsed)) continue;
    const count = parsed;
    if (!label || count <= 0) continue;
    countsByLabel.set(label, Math.max(countsByLabel.get(label) || 0, count));
  }

  return [...countsByLabel.entries()]
    .map(([label, count]) => ({
      label,
      count,
      severity: attemptSeverity(count, maxAttempts),
    }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
}

export function normalizeAttemptCountMap(
  attempts: Record<string, unknown>,
): Record<string, number> {
  const normalized: Record<string, number> = {};
  for (const row of normalizeAttemptCounts(attempts)) {
    normalized[row.label] = Math.max(normalized[row.label] || 0, row.count);
  }
  return normalized;
}

export function attemptLimit(maxAttempts: number): number {
  return Number.isFinite(maxAttempts) && maxAttempts > 0
    ? Math.floor(maxAttempts)
    : 3;
}

export function remainingAttempts(count: number, maxAttempts: number): number {
  return Math.max(attemptLimit(maxAttempts) - Math.max(0, Math.floor(count)), 0);
}

export function attemptSeverity(count: number, maxAttempts: number): AttemptSeverity {
  const threshold = attemptLimit(maxAttempts);
  if (count >= threshold) return "critical";
  if (threshold > 1 && count >= threshold - 1) return "warning";
  return "normal";
}
