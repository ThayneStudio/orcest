import type { ConsumerGroupInfo } from "../lib/types";

interface Props {
  groups: ConsumerGroupInfo[];
}

export function ConsumerGroups({ groups }: Props) {
  return (
    <div>
      <h2 className="text-sm font-medium text-zinc-400 mb-3">
        Consumer Groups ({groups.length})
      </h2>
      {groups.length === 0 ? (
        <div className="text-sm text-zinc-500 italic">No consumer groups</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-zinc-800 text-left text-zinc-500">
                <th className="pb-2 pr-4">Stream</th>
                <th className="pb-2 pr-4">Group</th>
                <th className="pb-2 pr-4">Consumers</th>
                <th className="pb-2">Pending</th>
              </tr>
            </thead>
            <tbody>
              {groups.map((g) => (
                <tr
                  key={`${g.stream}-${g.name}`}
                  className="border-b border-zinc-800/50"
                >
                  <td className="py-2 pr-4 font-mono">{g.stream}</td>
                  <td className="py-2 pr-4 text-zinc-400">{g.name}</td>
                  <td className="py-2 pr-4 font-mono">{g.consumers}</td>
                  <td className="py-2 font-mono">
                    <span
                      className={g.pending > 0 ? "text-yellow-400" : "text-zinc-400"}
                    >
                      {g.pending}
                    </span>
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
