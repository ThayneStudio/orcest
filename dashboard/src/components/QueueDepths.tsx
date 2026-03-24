import type { SystemSnapshot } from "../lib/types";

interface Props {
  snapshot: SystemSnapshot;
}

function depthColor(depth: number): string {
  if (depth === 0) return "text-emerald-400";
  if (depth <= 5) return "text-yellow-400";
  return "text-red-400";
}

function depthBg(depth: number): string {
  if (depth === 0) return "border-zinc-700";
  if (depth <= 5) return "border-yellow-500/30";
  return "border-red-500/30";
}

export function QueueDepths({ snapshot }: Props) {
  const taskQueues = Object.entries(snapshot.queue_depths).map(([name, depth]) => ({
    name,
    depth,
  }));

  const allQueues = [
    ...taskQueues,
    { name: "results", depth: snapshot.results_depth },
    { name: "dead-letter", depth: snapshot.dead_letter_count },
  ];

  return (
    <div>
      <h2 className="text-sm font-medium text-zinc-400 mb-3">Queue Depths</h2>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
        {taskQueues.length === 0 && (
          <div className="rounded-lg border border-zinc-700 bg-zinc-900 px-4 py-3">
            <div className="text-xs text-zinc-500">No task streams</div>
            <div className="text-2xl font-mono font-bold text-zinc-600">--</div>
          </div>
        )}
        {allQueues.map(({ name, depth }) => (
          <div
            key={name}
            className={`rounded-lg border bg-zinc-900 px-4 py-3 ${depthBg(depth)}`}
          >
            <div className="text-xs text-zinc-500 truncate" title={name}>
              {name}
            </div>
            <div className={`text-2xl font-mono font-bold ${depthColor(depth)}`}>
              {depth}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
