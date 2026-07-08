import type { SystemSnapshot } from "../lib/types";
import {
  attemptLimit,
  normalizeAttemptCounts,
  remainingAttempts,
  type AttemptCountRow,
  type AttemptSeverity,
} from "../lib/attempts";

interface Props {
  attempts: SystemSnapshot["attempt_counts"];
  maxAttempts: number;
  degraded?: boolean;
}

function severityTone(severity: AttemptSeverity): string {
  switch (severity) {
    case "critical":
      return "text-red-300";
    case "warning":
      return "text-yellow-300";
    default:
      return "text-zinc-400";
  }
}

function severityLabel(severity: AttemptSeverity): string {
  switch (severity) {
    case "critical":
      return "at max";
    case "warning":
      return "near max";
    default:
      return "retrying";
  }
}

function attemptWord(count: number): string {
  return count === 1 ? "attempt" : "attempts";
}

export function attemptBudgetLabel(count: number, maxAttempts: number): string {
  const remaining = remainingAttempts(count, maxAttempts);
  if (remaining <= 0) return "budget exhausted";
  return `${remaining} ${attemptWord(remaining)} left`;
}

export function attemptBudgetTitle(count: number, maxAttempts: number): string {
  const limit = attemptLimit(maxAttempts);
  const used = Math.max(0, Math.floor(count));
  const remaining = remainingAttempts(count, maxAttempts);
  const budget = remaining <= 0
    ? "retry budget exhausted"
    : `${remaining} ${attemptWord(remaining)} left`;
  return `${used} of ${limit} ${attemptWord(limit)} used; ${budget}`;
}

export function attemptCountsSectionStatus(
  degraded: boolean,
  rowCount: number,
): string | null {
  if (!degraded) return null;
  return rowCount > 0 ? "partial counters" : "unavailable";
}

export interface AttemptAttentionCounts {
  critical: number;
  warning: number;
}

export function attemptAttentionCounts(
  rows: Pick<AttemptCountRow, "severity">[],
): AttemptAttentionCounts {
  return rows.reduce(
    (counts, row) => {
      if (row.severity === "critical") counts.critical += 1;
      if (row.severity === "warning") counts.warning += 1;
      return counts;
    },
    { critical: 0, warning: 0 },
  );
}

export function attemptAttentionLabel(counts: AttemptAttentionCounts): string | null {
  const parts: string[] = [];
  if (counts.critical > 0) {
    parts.push(`${counts.critical} at max`);
  }
  if (counts.warning > 0) {
    parts.push(`${counts.warning} near max`);
  }
  return parts.length > 0 ? parts.join(", ") : null;
}

export const ATTEMPT_COUNT_CELL_CLASS =
  "max-w-[8rem] break-all px-4 py-2 text-right font-mono";

export function AttemptCounts({ attempts, maxAttempts, degraded = false }: Props) {
  const limit = attemptLimit(maxAttempts);
  const rows = normalizeAttemptCounts(attempts, maxAttempts);
  const attentionCounts = attemptAttentionCounts(rows);
  const sectionStatus = attemptCountsSectionStatus(degraded, rows.length);

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-medium text-zinc-400">
          Retry Attempts
        </h2>
        <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-right">
          {attentionCounts.critical > 0 && (
            <span className="text-xs text-red-300">
              {`${attentionCounts.critical} at max`}
            </span>
          )}
          {attentionCounts.warning > 0 && (
            <span className="text-xs text-yellow-300">
              {`${attentionCounts.warning} near max`}
            </span>
          )}
          {sectionStatus && (
            <span className="text-xs text-yellow-300">
              {sectionStatus}
            </span>
          )}
        </div>
      </div>
      {rows.length === 0 ? (
        <div className="rounded-lg border border-zinc-800 bg-zinc-900 px-4 py-3 text-sm text-zinc-500">
          {degraded ? "Retry counters unavailable" : "No retry counters reported"}
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg border border-zinc-800 bg-zinc-900">
          <table className="w-full text-sm" aria-label="Retry attempts">
            <thead>
              <tr className="border-b border-zinc-800 text-left text-zinc-500">
                <th scope="col" className="px-4 py-2 font-medium">Resource</th>
                <th scope="col" className="px-4 py-2 text-right font-medium">
                  Attempts / {limit}
                </th>
                <th scope="col" className="px-4 py-2 text-right font-medium">State</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.label} className="border-b border-zinc-800/50 last:border-0">
                  <th
                    scope="row"
                    className="max-w-[28rem] truncate px-4 py-2 text-left font-mono font-normal text-zinc-300"
                    title={row.label}
                  >
                    {row.label}
                  </th>
                  <td
                    className={`${ATTEMPT_COUNT_CELL_CLASS} ${severityTone(row.severity)}`}
                    title={attemptBudgetTitle(row.count, maxAttempts)}
                  >
                    {row.count}
                  </td>
                  <td
                    className={`px-4 py-2 text-right text-xs ${severityTone(row.severity)}`}
                    title={`${severityLabel(row.severity)}; ${attemptBudgetTitle(row.count, maxAttempts)}`}
                  >
                    {attemptBudgetLabel(row.count, maxAttempts)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
