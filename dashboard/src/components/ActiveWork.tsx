import type { LockInfo, StuckTask } from "../lib/types";
import { formatTtl } from "../lib/format";

interface Props {
  locks: LockInfo[];
  stuckTasks: StuckTask[];
}

export function ActiveWork({ locks, stuckTasks }: Props) {
  const stuckIds = new Set(
    stuckTasks.map((t) => t.resource_id)
  );

  return (
    <div>
      <h2 className="text-sm font-medium text-zinc-400 mb-3">
        Active Work ({locks.length})
      </h2>
      {locks.length === 0 ? (
        <div className="text-sm text-zinc-500 italic">No active locks</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-800 text-left text-zinc-500">
                <th className="pb-2 pr-4">Resource</th>
                <th className="pb-2 pr-4">Worker</th>
                <th className="pb-2 pr-4">TTL</th>
                <th className="pb-2">Status</th>
              </tr>
            </thead>
            <tbody>
              {locks.map((lock) => {
                // lock.resource is "repo:number", extract just the number
                const lockNum = lock.resource.split(":").pop() || lock.resource;
                const isStuck = stuckIds.has(lockNum);
                return (
                  <tr
                    key={lock.resource}
                    className="border-b border-zinc-800/50"
                  >
                    <td className="py-2 pr-4 font-mono">#{lock.resource}</td>
                    <td className="py-2 pr-4 text-zinc-400">{lock.owner}</td>
                    <td className="py-2 pr-4 font-mono">{formatTtl(lock.ttl)}</td>
                    <td className="py-2">
                      {isStuck ? (
                        <span className="inline-flex items-center rounded-full bg-red-500/20 px-2 py-0.5 text-xs text-red-400">
                          Stuck
                        </span>
                      ) : (
                        <span className="inline-flex items-center rounded-full bg-emerald-500/20 px-2 py-0.5 text-xs text-emerald-400">
                          Running
                        </span>
                      )}
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
