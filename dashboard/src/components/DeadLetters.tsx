import type { DeadLetterEntry } from "../lib/types";

interface Props {
  entries: DeadLetterEntry[];
  total: number;
}

function formatTimestamp(ms: number | null): string {
  if (ms === null) return "?";
  return new Date(ms).toLocaleString();
}

export function DeadLetters({ entries, total }: Props) {
  return (
    <div>
      <h2 className="text-sm font-medium text-zinc-400 mb-3">
        Dead Letters ({total} total)
      </h2>
      {entries.length === 0 ? (
        <div className="text-sm text-zinc-500 italic">
          No dead-lettered tasks
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-800 text-left text-zinc-500">
                <th className="pb-2 pr-4">Time</th>
                <th className="pb-2 pr-4">Type</th>
                <th className="pb-2 pr-4">Repo</th>
                <th className="pb-2 pr-4">Resource</th>
                <th className="pb-2">Reason</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((entry) => (
                <tr
                  key={entry.entry_id}
                  className="border-b border-zinc-800/50"
                >
                  <td className="py-2 pr-4 text-zinc-400">
                    {formatTimestamp(entry.timestamp_ms)}
                  </td>
                  <td className="py-2 pr-4">{entry.task_type}</td>
                  <td className="py-2 pr-4 text-zinc-400">{entry.repo}</td>
                  <td className="py-2 pr-4 font-mono">
                    {entry.resource_type} #{entry.resource_id}
                  </td>
                  <td className="py-2 text-red-400">
                    {entry.reason || "?"}
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
