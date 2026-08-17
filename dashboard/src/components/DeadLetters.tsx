import type { DeadLetterEntry } from "../lib/types";
import { resourceLabel } from "../lib/resource";
import { formatTimestampMs } from "../lib/format";
import { redisStreamDisplayName } from "../lib/queues";

interface Props {
  entries: DeadLetterEntry[];
  total: number;
  entriesDegraded?: boolean;
  depthDegraded?: boolean;
}

export function deadLetterEmptyMessage(
  total: number,
  _entriesDegraded = false,
  depthDegraded = false,
): string {
  if (depthDegraded && total === 0) return "Dead-letter data unavailable";
  return total > 0
    ? "Dead-letter entries exist, but recent details are unavailable"
    : "No dead-letter entries";
}

export function deadLetterHeadingText(
  total: number,
  loaded: number,
  entriesDegraded = false,
  depthDegraded = false,
): string {
  if (depthDegraded) {
    return loaded > 0
      ? `Dead Letters (${loaded} loaded, ? total)`
      : "Dead Letters (? total)";
  }
  const normalizedTotal = Math.max(loaded, total);
  if ((entriesDegraded && loaded > 0) || normalizedTotal > loaded) {
    return `Dead Letters (${loaded} loaded, ${normalizedTotal} total)`;
  }
  return `Dead Letters (${normalizedTotal} total)`;
}

export function deadLetterReasonText(reason: string | null): string {
  const trimmed = reason?.trim();
  return trimmed || "?";
}

export function deadLetterTaskIdDisplay(taskId: string | null, maxLength = 12): string {
  const trimmed = taskId?.trim();
  if (!trimmed) return "?";
  return trimmed.length <= maxLength ? trimmed : `${trimmed.slice(0, maxLength)}...`;
}

export function deadLetterTaskTypeText(taskType: string): string {
  switch (taskType.trim().toLowerCase()) {
    case "fix_pr":
      return "Fix PR";
    case "fix_ci":
      return "Fix CI";
    case "classify_ci":
      return "Classify CI";
    case "implement_issue":
      return "Implement";
    case "triage_followups":
      return "Triage";
    case "rebase_pr":
      return "Rebase";
    case "improve":
      return "Improve";
    default:
      return taskType.trim().replace(/[_-]+/g, " ") || "?";
  }
}

export function DeadLetters({
  entries,
  total,
  entriesDegraded = false,
  depthDegraded = false,
}: Props) {
  return (
    <div>
      <h2 className="text-sm font-medium text-zinc-400 mb-3">
        {deadLetterHeadingText(total, entries.length, entriesDegraded, depthDegraded)}
      </h2>
      {entries.length === 0 ? (
        <div className="text-sm text-zinc-500 italic">
          {deadLetterEmptyMessage(total, entriesDegraded, depthDegraded)}
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm" aria-label="Dead-letter entries">
            <thead>
              <tr className="border-b border-zinc-800 text-left text-zinc-500">
                <th scope="col" className="pb-2 pr-4">Time</th>
                <th scope="col" className="pb-2 pr-4">Stream</th>
                <th scope="col" className="pb-2 pr-4">Task</th>
                <th scope="col" className="pb-2 pr-4">Type</th>
                <th scope="col" className="pb-2 pr-4">Repo</th>
                <th scope="col" className="pb-2 pr-4">Resource</th>
                <th scope="col" className="pb-2">Reason</th>
              </tr>
            </thead>
            <tbody>
              {entries.map((entry) => {
                const label = resourceLabel(entry, { includeRepo: false });
                const fullLabel = resourceLabel(entry);
                const taskId = entry.task_id.trim();
                const taskIdDisplay = deadLetterTaskIdDisplay(taskId);
                const streamDisplay = redisStreamDisplayName(entry.dead_letter_stream);
                const taskTypeText = deadLetterTaskTypeText(entry.task_type);
                return (
                  <tr
                    key={entry.dead_letter_id}
                    className="border-b border-zinc-800/50"
                  >
                    <td className="py-2 pr-4 text-zinc-400">
                      {formatTimestampMs(entry.timestamp_ms)}
                    </td>
                    <td
                      className="max-w-[18rem] truncate py-2 pr-4 font-mono text-xs text-zinc-500"
                      title={entry.dead_letter_stream}
                    >
                      <span>{streamDisplay}</span>
                      <span className="sr-only"> raw stream {entry.dead_letter_stream}</span>
                    </td>
                    <td
                      className="max-w-[14rem] truncate py-2 pr-4 font-mono text-xs text-zinc-400"
                      title={taskId || undefined}
                    >
                      {taskId && taskIdDisplay !== taskId ? (
                        <>
                          <span aria-hidden="true">{taskIdDisplay}</span>
                          <span className="sr-only">{taskId}</span>
                        </>
                      ) : taskIdDisplay}
                    </td>
                    <td
                      className="max-w-[14rem] truncate py-2 pr-4"
                      title={entry.task_type || undefined}
                    >
                      {taskTypeText}
                    </td>
                    <td
                      className="max-w-[16rem] truncate py-2 pr-4 text-zinc-400"
                      title={entry.repo}
                    >
                      {entry.repo}
                    </td>
                    <th scope="row" className="max-w-[18rem] py-2 pr-4 text-left font-mono font-normal">
                      <div className="truncate" title={fullLabel}>
                        {label}
                        <span className="sr-only"> {entry.repo}</span>
                      </div>
                    </th>
                    <td
                      className="max-w-[32rem] break-words py-2 text-red-400"
                      title={deadLetterReasonText(entry.reason)}
                    >
                      {deadLetterReasonText(entry.reason)}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
