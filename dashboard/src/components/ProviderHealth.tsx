import type { SystemSnapshot } from "../lib/types";
import {
  providerMetricLabel,
  providerMetricSeverity,
  providerMetricTone,
  type ProviderMetricSeverity,
} from "../lib/providerHealth";

interface Props {
  health: SystemSnapshot["provider_health"];
  degraded?: boolean;
}

export function providerHealthSectionStatus(
  degraded: boolean,
  providerCount: number,
): string | null {
  if (!degraded) return null;
  return providerCount > 0 ? "partial counters" : "unavailable";
}

const PROVIDER_STATUS_RANK: Record<ProviderMetricSeverity, number> = {
  critical: 0,
  warning: 1,
  normal: 2,
};

export function providerHealthCardStatus(
  metrics: Array<[string, number]>,
): ProviderMetricSeverity {
  let status: ProviderMetricSeverity = "normal";
  for (const [metric, value] of metrics) {
    const severity = providerMetricSeverity(metric, value);
    if (severity === "critical") return "critical";
    if (severity === "warning") status = "warning";
  }
  return status;
}

export function providerHealthCardStatusLabel(
  status: ProviderMetricSeverity,
  metricCount: number,
): string {
  if (status === "critical") return "attention";
  if (status === "warning") return "warning";
  return `${metricCount} metric${metricCount === 1 ? "" : "s"}`;
}

export interface ProviderAttentionCounts {
  critical: number;
  warning: number;
}

export function providerHealthAttentionCounts(
  statuses: ProviderMetricSeverity[],
): ProviderAttentionCounts {
  return statuses.reduce(
    (counts, status) => {
      if (status === "critical") counts.critical += 1;
      if (status === "warning") counts.warning += 1;
      return counts;
    },
    { critical: 0, warning: 0 },
  );
}

export function providerHealthAttentionCount(statuses: ProviderMetricSeverity[]): number {
  const counts = providerHealthAttentionCounts(statuses);
  return counts.critical + counts.warning;
}

export function providerHealthCriticalLabel(count: number): string | null {
  if (count <= 0) return null;
  return count === 1 ? "1 needs attention" : `${count} need attention`;
}

export function providerHealthWarningLabel(count: number): string | null {
  if (count <= 0) return null;
  return count === 1 ? "1 warning" : `${count} warnings`;
}

function providerMetricStatusRank(metric: string, value: number): number {
  return PROVIDER_STATUS_RANK[providerMetricSeverity(metric, value)];
}

function providerHealthCardBorder(status: ProviderMetricSeverity): string {
  switch (status) {
    case "critical":
      return "border-red-500/30";
    case "warning":
      return "border-yellow-500/30";
    default:
      return "border-zinc-800";
  }
}

function providerHealthCardStatusClass(status: ProviderMetricSeverity): string {
  switch (status) {
    case "critical":
      return "text-xs text-red-300";
    case "warning":
      return "text-xs text-yellow-300";
    default:
      return "text-xs text-zinc-500";
  }
}

export const PROVIDER_METRIC_VALUE_CLASS =
  "min-w-0 max-w-[8rem] break-all text-right font-mono";

export function ProviderHealth({ health, degraded = false }: Props) {
  const providers = Object.entries(health || {})
    .map(([provider, metrics]) => {
      const sortedMetrics = Object.entries(metrics).sort(([aMetric, aValue], [bMetric, bValue]) =>
        providerMetricStatusRank(aMetric, aValue) - providerMetricStatusRank(bMetric, bValue) ||
        aMetric.localeCompare(bMetric)
      );
      return {
        provider,
        metrics: sortedMetrics,
        status: providerHealthCardStatus(sortedMetrics),
      };
    })
    .filter((row) => row.metrics.length > 0)
    .sort((a, b) =>
      PROVIDER_STATUS_RANK[a.status] - PROVIDER_STATUS_RANK[b.status] ||
      a.provider.localeCompare(b.provider)
    );
  const attentionCounts = providerHealthAttentionCounts(
    providers.map((provider) => provider.status),
  );
  const criticalLabel = providerHealthCriticalLabel(attentionCounts.critical);
  const warningLabel = providerHealthWarningLabel(attentionCounts.warning);
  const sectionStatus = providerHealthSectionStatus(degraded, providers.length);

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-medium text-zinc-400">
          Provider Health
        </h2>
        <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-right">
          {criticalLabel && (
            <span className="text-xs text-red-300">
              {criticalLabel}
            </span>
          )}
          {warningLabel && (
            <span className="text-xs text-yellow-300">
              {warningLabel}
            </span>
          )}
          {sectionStatus && (
            <span className="text-xs text-yellow-300">
              {sectionStatus}
            </span>
          )}
        </div>
      </div>
      {providers.length === 0 ? (
        <div className="rounded-lg border border-zinc-800 bg-zinc-900 px-4 py-3 text-sm text-zinc-500">
          {degraded ? "Provider counters unavailable" : "No provider counters reported"}
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-3">
          {providers.map(({ provider, metrics, status }) => {
            return (
              <div
                key={provider}
                className={`min-w-0 rounded-lg border bg-zinc-900 px-4 py-3 ${providerHealthCardBorder(status)}`}
              >
                <div className="mb-2 flex min-w-0 items-center justify-between gap-3">
                  <div
                    className="min-w-0 break-all font-mono text-sm text-zinc-200"
                    title={provider}
                  >
                    {provider}
                  </div>
                  <div className={providerHealthCardStatusClass(status)}>
                    {providerHealthCardStatusLabel(status, metrics.length)}
                  </div>
                </div>
                <div className="space-y-1.5">
                  {metrics.map(([metric, value]) => (
                    <div
                      key={metric}
                      className="flex min-w-0 items-center justify-between gap-3 text-sm"
                    >
                      <span className="min-w-0 truncate text-zinc-500" title={metric}>
                        {providerMetricLabel(metric)}
                      </span>
                      <span
                        className={`${PROVIDER_METRIC_VALUE_CLASS} ${providerMetricTone(metric, value)}`}
                        title={String(value)}
                      >
                        {value}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
