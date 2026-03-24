import { useState } from "react";
import type { RecentResult } from "../lib/types";
import { statusColor, formatDuration } from "../lib/format";

interface Props {
  results: RecentResult[];
}

export function RecentResults({ results }: Props) {
  const [expandedId, setExpandedId] = useState<string | null>(null);

  return (
    <div>
      <h2 className="text-sm font-medium text-zinc-400 mb-3">
        Recent Results ({results.length})
      </h2>
      {results.length === 0 ? (
        <div className="text-sm text-zinc-500 italic">No results yet</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-800 text-left text-zinc-500">
                <th className="pb-2 pr-4">Status</th>
                <th className="pb-2 pr-4">Type</th>
                <th className="pb-2 pr-4">Resource</th>
                <th className="pb-2 pr-4">Worker</th>
                <th className="pb-2 pr-4">Duration</th>
                <th className="pb-2">Summary</th>
              </tr>
            </thead>
            <tbody>
              {results.map((r) => (
                <tr
                  key={r.task_id}
                  className="border-b border-zinc-800/50 cursor-pointer hover:bg-zinc-800/30"
                  onClick={() =>
                    setExpandedId(expandedId === r.task_id ? null : r.task_id)
                  }
                >
                  <td className="py-2 pr-4">
                    <span
                      className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs ${statusColor(r.status)}`}
                    >
                      {r.status}
                    </span>
                  </td>
                  <td className="py-2 pr-4 text-zinc-400">{r.resource_type}</td>
                  <td className="py-2 pr-4 font-mono">#{r.resource_id}</td>
                  <td className="py-2 pr-4 text-zinc-400">{r.worker_id}</td>
                  <td className="py-2 pr-4 font-mono">
                    {formatDuration(r.duration_seconds)}
                  </td>
                  <td className="py-2 text-zinc-300">
                    {expandedId === r.task_id
                      ? r.summary
                      : r.summary.length > 80
                        ? r.summary.slice(0, 80) + "..."
                        : r.summary}
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
