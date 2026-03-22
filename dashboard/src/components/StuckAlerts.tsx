import type { StuckTask } from "../lib/types";

interface Props {
  stuckTasks: StuckTask[];
}

export function StuckAlerts({ stuckTasks }: Props) {
  if (stuckTasks.length === 0) return null;

  const critical = stuckTasks.filter((t) => t.severity === "critical");
  const warnings = stuckTasks.filter((t) => t.severity === "warning");

  return (
    <div className="space-y-2">
      {critical.length > 0 && (
        <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-red-400 font-medium">
              {critical.length} Critical
            </span>
          </div>
          <ul className="text-sm text-red-300 space-y-1">
            {critical.map((t) => (
              <li key={`${t.resource_type}-${t.resource_id}`}>
                {t.resource_type} #{t.resource_id}: {t.reason}
              </li>
            ))}
          </ul>
        </div>
      )}
      {warnings.length > 0 && (
        <div className="rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-4 py-3">
          <div className="flex items-center gap-2 mb-1">
            <span className="text-yellow-400 font-medium">
              {warnings.length} Warning{warnings.length !== 1 ? "s" : ""}
            </span>
          </div>
          <ul className="text-sm text-yellow-300 space-y-1">
            {warnings.map((t) => (
              <li key={`${t.resource_type}-${t.resource_id}`}>
                {t.resource_type} #{t.resource_id}: {t.reason}
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}
