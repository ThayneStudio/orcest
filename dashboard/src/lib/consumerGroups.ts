import type { ConsumerGroupInfo, QueuedTask } from "./types";

export const WORKER_CONSUMER_GROUP = "workers";

export function isWorkerConsumerGroup(group: ConsumerGroupInfo): boolean {
  return group.name === WORKER_CONSUMER_GROUP;
}

export function consumerGroupRoleLabel(group: ConsumerGroupInfo): "worker" | "aux" {
  return isWorkerConsumerGroup(group) ? "worker" : "aux";
}

export function consumerGroupRowKey(group: ConsumerGroupInfo): string {
  return JSON.stringify([consumerGroupStreamKey(group), group.name]);
}

export function consumerGroupCountTone(
  group: ConsumerGroupInfo,
  value: number | null,
): string {
  if (!isWorkerConsumerGroup(group)) return "text-zinc-500";
  if (value === null) return "text-yellow-300";
  return (value || 0) > 0 ? "text-yellow-400" : "text-zinc-400";
}

type ConsumerGroupQueueDepths = Record<string, number | undefined>;
type ConsumerGroupQueuedPreviews = Record<string, number | undefined>;
export type ConsumerGroupBacklogSource =
  | "consumer-backlog"
  | "queue-depth"
  | "queued-pending"
  | "queued-preview"
  | null;

export interface ConsumerGroupBacklogEvidence {
  count: number;
  source: ConsumerGroupBacklogSource;
  exact: boolean;
}

export interface ConsumerGroupNoConsumerBacklogSummary {
  groupCount: number;
  backlogCount: number;
  exact: boolean;
}

function nonNegativeCount(value: number | undefined): number {
  if (value === undefined) return 0;
  return Number.isFinite(value) && value > 0 ? value : 0;
}

export function consumerGroupStreamKey(group: Pick<ConsumerGroupInfo, "stream">): string {
  return group.stream.trim();
}

function lookupStreamCount(
  counts: Record<string, number | undefined> | undefined,
  stream: string,
): number {
  if (!counts) return 0;
  const trimmed = stream.trim();
  return nonNegativeCount(counts[trimmed] ?? counts[stream]);
}

export function queuedPreviewCountsByStream(
  queuedTasks: Array<Pick<QueuedTask, "stream">>,
): ConsumerGroupQueuedPreviews {
  const counts: Record<string, number> = {};
  for (const task of queuedTasks) {
    const stream = task.stream.trim();
    if (!stream) continue;
    counts[stream] = (counts[stream] || 0) + 1;
  }
  return counts;
}

export function consumerGroupBacklogEstimate(
  group: Pick<ConsumerGroupInfo, "pending" | "lag">,
  queuedPreviewCount = 0,
): number {
  const pending = nonNegativeCount(group.pending);
  const queuedPreview = nonNegativeCount(queuedPreviewCount);
  if (group.lag === null) return pending + queuedPreview;
  const lag = nonNegativeCount(group.lag);
  return Math.max(pending + lag, queuedPreview);
}

export function consumerGroupBacklogEvidence(
  group: Pick<ConsumerGroupInfo, "pending" | "lag">,
  evidence: { queueDepth?: number; queuedPreview?: number } = {},
): ConsumerGroupBacklogEvidence {
  const pending = nonNegativeCount(group.pending);
  const lag = group.lag === null ? 0 : nonNegativeCount(group.lag);
  const consumerBacklog = pending + lag;
  const queueDepth = nonNegativeCount(evidence.queueDepth);
  const queuedPreview = nonNegativeCount(evidence.queuedPreview);
  const inferredBacklog = group.lag === null
    ? pending + queuedPreview
    : Math.max(consumerBacklog, queuedPreview);
  const count = Math.max(queueDepth, inferredBacklog);

  if (count === 0) return { count, source: null, exact: true };
  if (queueDepth === count && queueDepth > 0) {
    return { count, source: "queue-depth", exact: true };
  }
  if (group.lag === null) {
    if (pending > 0 && queuedPreview > 0) {
      return { count, source: "queued-pending", exact: false };
    }
    if (queuedPreview > 0) {
      return { count, source: "queued-preview", exact: false };
    }
    if (pending > 0) {
      return { count, source: "consumer-backlog", exact: false };
    }
  }
  if (consumerBacklog === count && consumerBacklog > 0) {
    return { count, source: "consumer-backlog", exact: true };
  }
  return { count, source: "queued-preview", exact: false };
}

export function consumerGroupQueueDepth(
  group: ConsumerGroupInfo,
  queueDepths?: ConsumerGroupQueueDepths,
): number {
  return lookupStreamCount(queueDepths, group.stream);
}

export function consumerGroupQueuedPreviewCount(
  group: ConsumerGroupInfo,
  queuedPreviews?: ConsumerGroupQueuedPreviews,
): number {
  return lookupStreamCount(queuedPreviews, group.stream);
}

export function consumerGroupNeedsAttention(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreview = 0,
): boolean {
  if (!isWorkerConsumerGroup(group)) return false;
  return group.pending > 0 || (group.lag !== null && group.lag > 0) ||
    (group.consumers === 0 && (queueDepth > 0 || queuedPreview > 0));
}

function consumerGroupHasConfirmedBacklog(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreview = 0,
): boolean {
  return group.pending > 0 ||
    (group.lag !== null && group.lag > 0) ||
    queueDepth > 0 ||
    queuedPreview > 0;
}

export function consumerGroupHasNoConsumersWhileBacklogged(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreview = 0,
): boolean {
  return isWorkerConsumerGroup(group) &&
    group.consumers === 0 &&
    consumerGroupHasConfirmedBacklog(group, queueDepth, queuedPreview);
}

export function consumerGroupConsumerTone(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreview = 0,
): string {
  if (!isWorkerConsumerGroup(group)) return "text-zinc-500";
  if (consumerGroupHasNoConsumersWhileBacklogged(group, queueDepth, queuedPreview)) return "text-red-300";
  return group.consumers === 0 ? "text-zinc-500" : "text-zinc-400";
}

export function consumerGroupConsumerTitle(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreview = 0,
): string {
  if (consumerGroupHasNoConsumersWhileBacklogged(group, queueDepth, queuedPreview)) {
    return `${group.consumers} consumers, worker group has unhandled work`;
  }
  return String(group.consumers);
}

export function consumerGroupStatusLabel(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreview = 0,
): string | null {
  if (!isWorkerConsumerGroup(group)) return null;
  if (consumerGroupHasNoConsumersWhileBacklogged(group, queueDepth, queuedPreview)) return "no consumers";
  if (group.pending > 0) return "pending";
  if (group.lag === null) return "unknown lag";
  if (group.lag > 0) return "lagging";
  if (group.consumers === 0) return "idle";
  return "healthy";
}

export function consumerGroupStatusTone(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreview = 0,
): string {
  const label = consumerGroupStatusLabel(group, queueDepth, queuedPreview);
  if (label === "no consumers") return "bg-red-500/15 text-red-300";
  if (label === "pending" || label === "unknown lag" || label === "lagging") {
    return "bg-yellow-500/15 text-yellow-300";
  }
  if (label === "healthy") return "bg-emerald-500/15 text-emerald-300";
  if (label === "idle") return "bg-zinc-800 text-zinc-400";
  return "bg-zinc-800 text-zinc-500";
}

export function consumerGroupAttentionCount(
  groups: ConsumerGroupInfo[],
  queueDepths?: ConsumerGroupQueueDepths,
  queuedPreviews?: ConsumerGroupQueuedPreviews,
): number {
  return groups.filter((group) =>
    consumerGroupNeedsAttention(
      group,
      consumerGroupQueueDepth(group, queueDepths),
      consumerGroupQueuedPreviewCount(group, queuedPreviews),
    )
  ).length;
}

export function consumerGroupNoConsumerBacklogCount(
  groups: ConsumerGroupInfo[],
  queueDepths?: ConsumerGroupQueueDepths,
  queuedPreviews?: ConsumerGroupQueuedPreviews,
): number {
  return groups.filter((group) =>
    consumerGroupHasNoConsumersWhileBacklogged(
      group,
      consumerGroupQueueDepth(group, queueDepths),
      consumerGroupQueuedPreviewCount(group, queuedPreviews),
    )
  ).length;
}

export function consumerGroupNoConsumerBacklogSummary(
  groups: ConsumerGroupInfo[],
  queueDepths?: ConsumerGroupQueueDepths,
  queuedPreviews?: ConsumerGroupQueuedPreviews,
): ConsumerGroupNoConsumerBacklogSummary {
  let groupCount = 0;
  let backlogCount = 0;
  let exact = true;

  for (const group of groups) {
    const queueDepth = consumerGroupQueueDepth(group, queueDepths);
    const queuedPreview = consumerGroupQueuedPreviewCount(group, queuedPreviews);
    if (!consumerGroupHasNoConsumersWhileBacklogged(group, queueDepth, queuedPreview)) continue;
    const evidence = consumerGroupBacklogEvidence(group, { queueDepth, queuedPreview });
    groupCount += 1;
    backlogCount += evidence.count;
    exact = exact && evidence.exact;
  }

  return { groupCount, backlogCount, exact };
}

export function consumerGroupAttentionLabel(
  count: number,
  degraded = false,
  noConsumerCount = 0,
  noConsumerBacklogCount = 0,
  noConsumerBacklogExact = true,
): string | null {
  if (noConsumerCount > 0) {
    const groupLabel = noConsumerCount === 1 ? "worker group has" : "worker groups have";
    const backlogSuffix = noConsumerBacklogCount > 0
      ? ` (${noConsumerBacklogExact ? "" : "at least "}${noConsumerBacklogCount} queued/pending)`
      : "";
    const remaining = Math.max(0, count - noConsumerCount);
    const suffix = remaining > 0
      ? `; ${remaining} more ${remaining === 1 ? "needs" : "need"} attention`
      : "";
    return `${noConsumerCount} ${groupLabel} no consumers${backlogSuffix}${suffix}`;
  }
  if (count > 0) {
    const groupLabel = count === 1 ? "worker group needs" : "worker groups need";
    return `${count} ${groupLabel} attention`;
  }
  return degraded ? "partial data" : null;
}

function consumerGroupSortRank(
  group: ConsumerGroupInfo,
  queueDepth = 0,
  queuedPreview = 0,
): number {
  if (consumerGroupNeedsAttention(group, queueDepth, queuedPreview)) return 0;
  if (isWorkerConsumerGroup(group)) return 1;
  return 2;
}

export function sortConsumerGroups(
  groups: ConsumerGroupInfo[],
  queueDepths?: ConsumerGroupQueueDepths,
  queuedPreviews?: ConsumerGroupQueuedPreviews,
): ConsumerGroupInfo[] {
  return [...groups].sort((a, b) => {
    const rankCompare =
      consumerGroupSortRank(
        a,
        consumerGroupQueueDepth(a, queueDepths),
        consumerGroupQueuedPreviewCount(a, queuedPreviews),
      ) -
      consumerGroupSortRank(
        b,
        consumerGroupQueueDepth(b, queueDepths),
        consumerGroupQueuedPreviewCount(b, queuedPreviews),
      );
    if (rankCompare !== 0) return rankCompare;
    const streamCompare = a.stream.localeCompare(b.stream);
    if (streamCompare !== 0) return streamCompare;
    if (isWorkerConsumerGroup(a) && !isWorkerConsumerGroup(b)) return -1;
    if (!isWorkerConsumerGroup(a) && isWorkerConsumerGroup(b)) return 1;
    return a.name.localeCompare(b.name);
  });
}
