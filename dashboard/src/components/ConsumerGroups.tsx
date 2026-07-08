import type { ConsumerGroupInfo, QueuedTask } from "../lib/types";
import { degradedCountLabel } from "../lib/counts";
import {
  consumerGroupAttentionCount,
  consumerGroupAttentionLabel,
  consumerGroupBacklogEvidence,
  consumerGroupConsumerTitle,
  consumerGroupConsumerTone,
  consumerGroupCountTone,
  consumerGroupNoConsumerBacklogSummary,
  consumerGroupQueueDepth,
  consumerGroupRowKey,
  consumerGroupRoleLabel,
  consumerGroupQueuedPreviewCount,
  consumerGroupStatusLabel,
  consumerGroupStatusTone,
  isWorkerConsumerGroup,
  queuedPreviewCountsByStream,
  sortConsumerGroups,
} from "../lib/consumerGroups";
import { queueStreamDisplayName } from "../lib/queues";

interface Props {
  groups: ConsumerGroupInfo[];
  queueDepths?: Record<string, number>;
  queuedTasks?: QueuedTask[];
  degraded?: boolean;
}

export const CONSUMER_GROUP_COUNT_CELL_CLASS =
  "max-w-[8rem] break-all py-2 pr-4 font-mono";
export const CONSUMER_GROUP_BACKLOG_CELL_CLASS =
  "max-w-[8rem] break-all py-2 pr-4 font-mono";
export const CONSUMER_GROUP_LAG_CELL_CLASS =
  "max-w-[8rem] break-all py-2 font-mono";

function nonNegativeCount(value: number | undefined): number {
  if (value === undefined) return 0;
  return Number.isFinite(value) && value > 0 ? value : 0;
}

export function consumerGroupBacklogCount(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreviewCount = 0,
): number {
  return consumerGroupBacklogEvidence(group, {
    queueDepth,
    queuedPreview: queuedPreviewCount,
  }).count;
}

export function consumerGroupBacklogText(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreviewCount = 0,
): string {
  if (!isWorkerConsumerGroup(group)) return "-";
  const backlog = consumerGroupBacklogCount(group, queueDepth, queuedPreviewCount);
  if (backlog > 0) return String(backlog);
  return group.lag === null ? "?" : "0";
}

export function consumerGroupBacklogTitle(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreviewCount = 0,
): string {
  if (!isWorkerConsumerGroup(group)) {
    return "auxiliary group";
  }

  const pending = nonNegativeCount(group.pending);
  const lag = group.lag === null ? 0 : nonNegativeCount(group.lag);
  const depth = nonNegativeCount(queueDepth);
  const queuedPreview = nonNegativeCount(queuedPreviewCount);
  const evidence = [
    `pending ${pending}`,
    group.lag === null ? "lag unknown" : `lag ${lag}`,
  ];
  if (depth > 0) evidence.push(`queue depth ${depth}`);
  if (queuedPreview > 0) {
    evidence.push(`${queuedPreview} queued ${queuedPreview === 1 ? "preview" : "previews"}`);
  }

  const backlog = consumerGroupBacklogEvidence(group, {
    queueDepth: depth,
    queuedPreview,
  });
  const prefix = backlog.count > 0
    ? `${backlog.exact ? "" : "at least "}${backlog.count} queued/pending`
    : "no confirmed backlog";
  return `${prefix}; ${evidence.join(", ")}`;
}

export function consumerGroupBacklogTone(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreviewCount = 0,
): string {
  if (!isWorkerConsumerGroup(group)) return "text-zinc-600";
  if (consumerGroupConsumerTone(group, queueDepth, queuedPreviewCount) === "text-red-300") {
    return "text-red-300";
  }
  if (consumerGroupBacklogCount(group, queueDepth, queuedPreviewCount) > 0) {
    return "text-yellow-400";
  }
  return group.lag === null ? "text-yellow-300" : "text-zinc-400";
}

export function ConsumerGroups({
  groups,
  queueDepths = {},
  queuedTasks = [],
  degraded = false,
}: Props) {
  const queuedPreviews = queuedPreviewCountsByStream(queuedTasks);
  const rows = sortConsumerGroups(groups, queueDepths, queuedPreviews);
  const attentionCount = consumerGroupAttentionCount(rows, queueDepths, queuedPreviews);
  const noConsumerBacklog = consumerGroupNoConsumerBacklogSummary(rows, queueDepths, queuedPreviews);
  const noConsumerCount = noConsumerBacklog.groupCount;
  const attentionLabel = consumerGroupAttentionLabel(
    attentionCount,
    degraded,
    noConsumerCount,
    noConsumerBacklog.backlogCount,
    noConsumerBacklog.exact,
  );
  const queueDepthForGroup = (group: ConsumerGroupInfo) =>
    consumerGroupQueueDepth(group, queueDepths);
  const queuedPreviewCountForGroup = (group: ConsumerGroupInfo) =>
    consumerGroupQueuedPreviewCount(group, queuedPreviews);

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-medium text-zinc-400">
          Consumer Groups ({degradedCountLabel(rows.length, degraded)})
        </h2>
        {attentionLabel && (
          <span className={
            noConsumerCount > 0
              ? "text-xs text-red-300"
              : attentionCount > 0
                ? "text-xs text-yellow-300"
                : "text-xs text-zinc-500"
          }>
            {attentionLabel}
          </span>
        )}
      </div>
      {rows.length === 0 ? (
        <div className="text-sm text-zinc-500 italic">
          {degraded ? "Consumer groups unavailable" : "No consumer groups"}
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm" aria-label="Consumer groups">
            <thead>
              <tr className="border-b border-zinc-800 text-left text-zinc-500">
                <th scope="col" className="pb-2 pr-4">Stream</th>
                <th scope="col" className="pb-2 pr-4">Group</th>
                <th scope="col" className="pb-2 pr-4">Role</th>
                <th scope="col" className="pb-2 pr-4">Status</th>
                <th
                  scope="col"
                  className="pb-2 pr-4"
                  title="Highest backlog evidence from pending, lag, queue depth, or queued previews"
                >
                  Backlog
                </th>
                <th scope="col" className="pb-2 pr-4">Consumers</th>
                <th scope="col" className="pb-2 pr-4">Pending</th>
                <th scope="col" className="pb-2">Lag</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((g) => {
                const queueDepth = queueDepthForGroup(g);
                const queuedPreviewCount = queuedPreviewCountForGroup(g);
                const statusLabel = consumerGroupStatusLabel(g, queueDepth, queuedPreviewCount);
                return (
                  <tr
                    key={consumerGroupRowKey(g)}
                    className="border-b border-zinc-800/50"
                  >
                  <td
                    className="max-w-[20rem] truncate py-2 pr-4 font-mono"
                    title={g.stream}
                  >
                    <span>{queueStreamDisplayName(g.stream)}</span>
                    <span className="sr-only"> raw stream {g.stream}</span>
                  </td>
                  <th
                    scope="row"
                    className="max-w-[16rem] truncate py-2 pr-4 text-left font-normal text-zinc-400"
                    title={`${g.name} on ${g.stream}`}
                  >
                    {g.name}
                    <span className="sr-only"> on {g.stream}</span>
                  </th>
                  <td className="py-2 pr-4">
                    <span className={
                      consumerGroupRoleLabel(g) === "worker"
                        ? "inline-flex rounded-full bg-blue-500/15 px-2 py-0.5 text-xs text-blue-300"
                        : "inline-flex rounded-full bg-zinc-800 px-2 py-0.5 text-xs text-zinc-500"
                    }>
                      {consumerGroupRoleLabel(g)}
                    </span>
                  </td>
                  <td className="py-2 pr-4">
                    {statusLabel ? (
                      <span
                        className={`inline-flex whitespace-nowrap rounded-full px-2 py-0.5 text-xs ${consumerGroupStatusTone(g, queueDepth, queuedPreviewCount)}`}
                      >
                        {statusLabel}
                      </span>
                    ) : (
                      <span className="text-zinc-600">-</span>
                    )}
                  </td>
                  <td
                    className={CONSUMER_GROUP_BACKLOG_CELL_CLASS}
                    title={consumerGroupBacklogTitle(g, queueDepth, queuedPreviewCount)}
                  >
                    <span className={consumerGroupBacklogTone(g, queueDepth, queuedPreviewCount)}>
                      {consumerGroupBacklogText(g, queueDepth, queuedPreviewCount)}
                    </span>
                  </td>
                  <td
                    className={CONSUMER_GROUP_COUNT_CELL_CLASS}
                    title={consumerGroupConsumerTitle(g, queueDepth, queuedPreviewCount)}
                  >
                    <span className={consumerGroupConsumerTone(g, queueDepth, queuedPreviewCount)}>
                      {g.consumers}
                    </span>
                  </td>
                  <td
                    className={CONSUMER_GROUP_COUNT_CELL_CLASS}
                    title={String(g.pending)}
                  >
                    <span
                      className={consumerGroupCountTone(g, g.pending)}
                    >
                      {g.pending}
                    </span>
                  </td>
                  <td
                    className={CONSUMER_GROUP_LAG_CELL_CLASS}
                    title={g.lag === null ? "unknown lag" : String(g.lag)}
                  >
                    <span
                      className={consumerGroupCountTone(g, g.lag)}
                    >
                      {g.lag === null ? "?" : g.lag}
                    </span>
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
