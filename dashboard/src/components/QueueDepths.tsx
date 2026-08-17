import type { SystemSnapshot } from "../lib/types";
import {
  consumerGroupBacklogEvidence,
  consumerGroupHasNoConsumersWhileBacklogged,
  consumerGroupQueueDepth,
  consumerGroupQueuedPreviewCount,
  isWorkerConsumerGroup,
  queuedPreviewCountsByStream,
} from "../lib/consumerGroups";
import {
  isTaskStreamName,
  normalizeDepth,
  normalizeQueueDepths,
  queueStreamDisplayName,
} from "../lib/queues";
import { degradedCountLabel } from "../lib/counts";

interface Props {
  snapshot: SystemSnapshot;
  degraded?: boolean;
  resultsDepthDegraded?: boolean;
  deadLetterDepthDegraded?: boolean;
}

export type QueueDepthEvidence = "depth" | "consumer-backlog" | "queued-pending" | "queued-preview";

export interface QueueDepthTaskRow {
  name: string;
  depth: number;
  evidence: QueueDepthEvidence;
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

function retainedColor(): string {
  return "text-zinc-300";
}

function degradedColor(): string {
  return "text-yellow-300";
}

function retainedBg(): string {
  return "border-zinc-800";
}

function degradedBg(): string {
  return "border-yellow-500/30";
}

export const QUEUE_DEPTH_VALUE_CLASS =
  "min-w-0 break-all font-mono text-xl font-bold leading-tight sm:text-2xl";

export function queueDepthDisplayValue(depth: number, degraded: boolean): string {
  return degradedCountLabel(depth, degraded);
}

export function queueDepthDetailLabel(
  isTaskQueue: boolean,
  depth: number,
  degraded: boolean,
  aggregate = false,
  evidence: QueueDepthEvidence = "depth",
): string {
  if (degraded) return depth > 0 ? "partial" : "unavailable";
  if (aggregate) return "retained total";
  if (isTaskQueue && evidence === "consumer-backlog") return "consumer backlog";
  if (isTaskQueue && evidence === "queued-pending") return "queued/pending";
  if (isTaskQueue && evidence === "queued-preview") {
    return depth === 1 ? "queued preview" : "queued previews";
  }
  return isTaskQueue ? "pending + lag" : "retained";
}

export function queueBacklogSummaryLabel(
  activeQueueCount: number,
  backlogTotal: number,
  degraded = false,
  exact = true,
): string | null {
  const total = normalizeDepth(backlogTotal);
  if (total === 0) return null;

  const activeCount = Math.max(1, normalizeDepth(activeQueueCount));
  const streamLabel = activeCount === 1 ? "stream" : "streams";
  const totalLabel = queueDepthDisplayValue(total, degraded);
  if (degraded) return `${totalLabel} across ${activeCount} ${streamLabel}`;
  const prefix = exact ? "" : "at least ";
  return `${prefix}${totalLabel} queued/pending across ${activeCount} ${streamLabel}`;
}

export function queueNoConsumerSummaryLabel(streamDisplayNames: string[]): string | null {
  const names = streamDisplayNames
    .map((name) => name.trim())
    .filter(Boolean);
  if (names.length === 0) return null;
  if (names.length === 1) return `${names[0]} has no consumers`;
  if (names.length <= 3) {
    const namedStreams = names.length === 2
      ? `${names[0]} and ${names[1]}`
      : `${names[0]}, ${names[1]}, and ${names[2]}`;
    return `${namedStreams} have no consumers`;
  }
  return `${names[0]}, ${names[1]}, and ${names.length - 2} more streams have no consumers`;
}

function queueDepthEvidenceRank(evidence: QueueDepthEvidence): number {
  switch (evidence) {
    case "depth": return 0;
    case "consumer-backlog": return 1;
    case "queued-pending": return 2;
    case "queued-preview": return 2;
  }
}

export function queueDepthTaskRows(
  snapshot: Pick<SystemSnapshot, "queue_depths" | "queued_tasks" | "consumer_groups">,
): QueueDepthTaskRow[] {
  const queuedPreviewsByStream = queuedPreviewCountsByStream(snapshot.queued_tasks);
  const rowsByName = new Map<string, QueueDepthTaskRow>();

  for (const queue of normalizeQueueDepths(snapshot.queue_depths)) {
    const queuedPreview = normalizeDepth(queuedPreviewsByStream[queue.name]);
    rowsByName.set(queue.name, {
      name: queue.name,
      depth: Math.max(queue.depth, queuedPreview),
      evidence: queuedPreview > queue.depth ? "queued-preview" : "depth",
    });
  }

  const upsertEvidenceRow = (
    rawName: string,
    depth: number,
    evidence: QueueDepthEvidence,
  ) => {
    const name = rawName.trim();
    const normalizedDepth = normalizeDepth(depth);
    if (!name || !isTaskStreamName(name) || normalizedDepth === 0) return;

    const existing = rowsByName.get(name);
    if (!existing) {
      rowsByName.set(name, { name, depth: normalizedDepth, evidence });
      return;
    }

    if (normalizedDepth > existing.depth) {
      existing.depth = normalizedDepth;
      existing.evidence = evidence;
    }
  };

  for (const group of snapshot.consumer_groups) {
    const stream = group.stream.trim();
    const queuedPreview = normalizeDepth(queuedPreviewsByStream[stream]);
    const backlog = consumerGroupBacklogEvidence(group, { queuedPreview });
    if (!isWorkerConsumerGroup(group) || backlog.count === 0) continue;
    const evidence =
      backlog.source === "consumer-backlog" || backlog.source === "queued-pending"
        ? backlog.source
        : "queued-preview";
    upsertEvidenceRow(
      stream,
      backlog.count,
      evidence,
    );
  }

  for (const [stream, count] of Object.entries(queuedPreviewsByStream)) {
    upsertEvidenceRow(stream, normalizeDepth(count), "queued-preview");
  }

  return [...rowsByName.values()].sort((a, b) =>
    b.depth - a.depth ||
    queueDepthEvidenceRank(a.evidence) - queueDepthEvidenceRank(b.evidence) ||
    a.name.localeCompare(b.name)
  );
}

export function QueueDepths({
  snapshot,
  degraded = false,
  resultsDepthDegraded = false,
  deadLetterDepthDegraded = false,
}: Props) {
  const taskQueues = queueDepthTaskRows(snapshot);
  const taskBacklogTotal = taskQueues.reduce(
    (total, queue) => total + queue.depth,
    0,
  );
  const activeTaskQueueCount = taskQueues.filter((queue) => queue.depth > 0).length;
  const taskBacklogIsExact = taskQueues.every((queue) => queue.evidence === "depth");
  const taskBacklogLabel = queueBacklogSummaryLabel(
    activeTaskQueueCount,
    taskBacklogTotal,
    degraded,
    taskBacklogIsExact,
  );
  const queuedPreviewsByStream = queuedPreviewCountsByStream(snapshot.queued_tasks);
  const noConsumerStreams = new Set(
    snapshot.consumer_groups
      .filter((group) => consumerGroupHasNoConsumersWhileBacklogged(
        group,
        taskQueues.find((queue) => queue.name === group.stream.trim())?.depth ||
          consumerGroupQueueDepth(group, snapshot.queue_depths) ||
          0,
        consumerGroupQueuedPreviewCount(group, queuedPreviewsByStream),
      ))
      .map((group) => group.stream.trim())
      .filter(Boolean),
  );
  const noConsumerBacklogQueues = taskQueues.filter((queue) => (
    noConsumerStreams.has(queue.name)
  ));
  const noConsumerBacklogLabel = queueNoConsumerSummaryLabel(
    noConsumerBacklogQueues.map((queue) => queueStreamDisplayName(queue.name)),
  );

  const allQueues = [
    ...taskQueues.map((queue) => ({
      key: queue.name,
      name: queue.name,
      displayName: queueStreamDisplayName(queue.name),
      title: queue.name,
      assistiveLabel: ` raw stream ${queue.name}`,
      depth: queue.depth,
      evidence: queue.evidence,
      degraded,
      aggregate: false,
      noConsumers: noConsumerStreams.has(queue.name) &&
        queue.depth > 0,
    })),
    {
      key: "results:aggregate",
      name: "results",
      displayName: "all results",
      title: "Aggregated retained results across configured Redis prefixes",
      assistiveLabel: " aggregate retained results",
      depth: normalizeDepth(snapshot.results_depth),
      evidence: "depth" as QueueDepthEvidence,
      degraded: resultsDepthDegraded,
      aggregate: true,
      noConsumers: false,
    },
    {
      key: "dead-letter:aggregate",
      name: "dead-letter",
      displayName: "all dead-letter entries",
      title: "Aggregated retained dead-letter entries across configured Redis prefixes",
      assistiveLabel: " aggregate retained dead-letter entries",
      depth: normalizeDepth(snapshot.dead_letter_count),
      evidence: "depth" as QueueDepthEvidence,
      degraded: deadLetterDepthDegraded,
      aggregate: true,
      noConsumers: false,
    },
  ];
  const hasDegradedDepth = degraded || resultsDepthDegraded || deadLetterDepthDegraded;

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-medium text-zinc-400">
          Worker Queue & Stream Depths
        </h2>
        <div className="flex flex-wrap items-center justify-end gap-x-3 gap-y-1 text-right">
          {taskBacklogLabel && (
            <span
              className={`text-xs ${
                degraded
                  ? "text-yellow-300"
                  : taskBacklogTotal <= 5
                    ? "text-yellow-300"
                    : "text-red-300"
              }`}
            >
              {taskBacklogLabel}
            </span>
          )}
          {noConsumerBacklogLabel && (
            <span className="text-xs text-red-300">
              {noConsumerBacklogLabel}
            </span>
          )}
          {hasDegradedDepth && (
            <span className="text-xs text-yellow-300">
              some depths unavailable
            </span>
          )}
        </div>
      </div>
      <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-3">
        {taskQueues.length === 0 && (
          <div className="min-w-0 rounded-lg border border-zinc-700 bg-zinc-900 px-4 py-3">
            <div className="text-xs text-zinc-500">
              {degraded ? "Task stream depths unavailable" : "No task streams"}
            </div>
            <div className="text-2xl font-mono font-bold text-zinc-600">--</div>
          </div>
        )}
        {allQueues.map(({
          key,
          name,
          displayName,
          title,
          assistiveLabel,
          depth,
          evidence,
          degraded: depthDegraded,
          aggregate,
          noConsumers,
        }) => {
          const isTaskQueue = isTaskStreamName(name);
          const depthValue = queueDepthDisplayValue(depth, depthDegraded);
          return (
            <div
              key={key}
              className={`min-w-0 rounded-lg border bg-zinc-900 px-4 py-3 ${
                depthDegraded
                  ? degradedBg()
                  : noConsumers
                    ? "border-red-500/40"
                    : isTaskQueue
                      ? depthBg(depth)
                      : retainedBg()
              }`}
            >
              <div className="truncate text-xs text-zinc-500" title={title}>
                <span>{displayName}</span>
                <span className="sr-only">{assistiveLabel}</span>
              </div>
              <div className={`${QUEUE_DEPTH_VALUE_CLASS} ${
                depthDegraded ? degradedColor() : isTaskQueue ? depthColor(depth) : retainedColor()
              }`} title={depthValue}>
                {depthValue}
              </div>
              <div className={`mt-1 text-xs ${
                depthDegraded ? "text-yellow-300/70" : "text-zinc-600"
              }`}>
                {queueDepthDetailLabel(isTaskQueue, depth, depthDegraded, aggregate, evidence)}
              </div>
              {noConsumers && (
                <div className="mt-2 inline-flex rounded-full bg-red-500/15 px-2 py-0.5 text-xs text-red-300">
                  no consumers
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
