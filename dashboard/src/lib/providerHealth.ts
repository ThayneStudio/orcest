export type ProviderMetricSeverity = "normal" | "warning" | "critical";

const METRIC_LABELS: Record<string, string> = {
  credential_refresh_failures: "Credential refresh failures",
  exhausted_skip: "Exhausted skips",
  refresh_failures: "Refresh failures",
  rebake_required_failures: "Rebake failures",
};

export function providerMetricLabel(metric: string): string {
  return METRIC_LABELS[metric] || metric.replaceAll("_", " ");
}

function isWarningProviderMetric(metric: string): boolean {
  return metric === "exhausted_skip" ||
    metric.endsWith("_failure") ||
    metric.endsWith("_failures");
}

export function providerMetricSeverity(metric: string, value: number): ProviderMetricSeverity {
  if (!Number.isFinite(value) || value <= 0) return "normal";
  if (metric === "rebake_required_failures") return "critical";
  return isWarningProviderMetric(metric) ? "warning" : "normal";
}

export function providerMetricTone(metric: string, value: number): string {
  switch (providerMetricSeverity(metric, value)) {
    case "critical":
      return "text-red-300";
    case "warning":
      return "text-yellow-300";
    default:
      return "text-zinc-500";
  }
}
