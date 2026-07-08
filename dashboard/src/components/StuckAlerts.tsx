import type { ConsumerGroupInfo, QueuedTask, StuckTask } from "../lib/types";
import {
  consumerGroupBacklogEvidence,
  consumerGroupHasNoConsumersWhileBacklogged,
  consumerGroupQueueDepth,
  consumerGroupQueuedPreviewCount,
  consumerGroupStreamKey,
  queuedPreviewCountsByStream,
} from "../lib/consumerGroups";
import { redisStreamDisplayName } from "../lib/queues";
import { stuckTaskAlertKey, stuckTaskResourceLabel } from "../lib/resource";

interface Props {
  stuckTasks: StuckTask[];
  queuedTasks?: QueuedTask[];
  consumerGroups?: ConsumerGroupInfo[];
  queueDepths?: Record<string, number>;
  degraded?: boolean;
}

export function stuckTaskReasonText(reason: string): string {
  const trimmed = reason.trim();
  return trimmed || "reason unavailable";
}

export function stuckTaskSeverityGroupLabel(
  count: number,
  severity: StuckTask["severity"],
  groupedCount = count,
): string {
  if (groupedCount < count) {
    const resourceLabel = groupedCount === 1 ? "stuck resource" : "stuck resources";
    const occurrenceLabel = count === 1 ? "occurrence" : "occurrences";
    return `${groupedCount} ${severity} ${resourceLabel} (${count} ${occurrenceLabel})`;
  }
  const taskLabel = count === 1 ? "stuck task" : "stuck tasks";
  return `${count} ${severity} ${taskLabel}`;
}

export function stuckTaskSeverityGroupsLabel(
  groups: StuckTask[][],
  severity: StuckTask["severity"],
  summaries: StuckTaskQueueSummary[] = [],
): string {
  const count = groups.reduce((total, group) => total + group.length, 0);
  const queueGroupCount = groups.filter(stuckTaskGroupIsNoConsumerBacklog).length;
  const backlogTotal = summaries.reduce((total, summary) =>
    total + Math.max(summary.count, summary.backlogCount),
  0);
  if (
    queueGroupCount > 0 &&
    queueGroupCount === groups.length &&
    (groups.length < count || backlogTotal > count)
  ) {
    const shownResourceCount = groups.reduce(
      (total, group) => total + uniqueResourceLabels(group).length,
      0,
    );
    const queueLabel = groups.length === 1 ? "stuck queue" : "stuck queues";
    const resourceLabel = shownResourceCount === 1 ? "shown resource" : "shown resources";
    if (backlogTotal > count) {
      const exact = summaries.every((summary) =>
        summary.backlogCount <= summary.count || summary.backlogExact
      );
      const prefix = exact ? "" : "at least ";
      return `${groups.length} ${severity} ${queueLabel} (${prefix}${backlogTotal} queued/pending; ${shownResourceCount} ${resourceLabel})`;
    }
    return `${groups.length} ${severity} ${queueLabel} (${shownResourceCount} ${resourceLabel})`;
  }
  if (queueGroupCount > 0 && groups.length < count) {
    const occurrenceLabel = count === 1 ? "occurrence" : "occurrences";
    return `${groups.length} ${severity} stuck groups (${count} ${occurrenceLabel})`;
  }
  return stuckTaskSeverityGroupLabel(count, severity, groups.length);
}

export function stuckTaskGroupKey(task: StuckTask): string {
  return JSON.stringify([
    task.prefix || "",
    task.resource_type,
    task.repo || "",
    task.resource_id,
    task.reason,
    task.severity,
  ]);
}

function stuckTaskResourceKey(task: StuckTask): string {
  return JSON.stringify([
    task.prefix || "",
    task.resource_type,
    task.repo || "",
    task.resource_id,
  ]);
}

export function stuckTaskIsNoConsumerBacklog(task: StuckTask): boolean {
  const stream = task.stream?.trim();
  const consumerGroup = task.consumer_group?.trim();
  return Boolean(stream && consumerGroup && task.no_worker_consumers);
}

function noConsumerBacklogQueueGroupKey(task: StuckTask): string | null {
  if (!stuckTaskIsNoConsumerBacklog(task)) return null;
  const stream = task.stream?.trim() || "";
  const consumerGroup = task.consumer_group?.trim() || "";

  return JSON.stringify([
    "no-consumer-backlog",
    stream,
    consumerGroup,
    task.severity,
  ]);
}

export function groupedStuckTasks(tasks: StuckTask[]): StuckTask[][] {
  const queueBacklogGroups = new Map<string, Set<string>>();
  for (const task of tasks) {
    const key = noConsumerBacklogQueueGroupKey(task);
    if (!key) continue;
    const resources = queueBacklogGroups.get(key) ?? new Set<string>();
    resources.add(stuckTaskResourceKey(task));
    queueBacklogGroups.set(key, resources);
  }
  const aggregateQueueKeys = new Set(
    [...queueBacklogGroups.entries()]
      .filter(([, resources]) => resources.size > 1)
      .map(([key]) => key),
  );

  const groups = new Map<string, StuckTask[]>();
  for (const task of tasks) {
    const queueKey = noConsumerBacklogQueueGroupKey(task);
    const key = queueKey && aggregateQueueKeys.has(queueKey)
      ? queueKey
      : stuckTaskGroupKey(task);
    groups.set(key, [...(groups.get(key) || []), task]);
  }
  return [...groups.values()];
}

export function stuckTaskGroupIsNoConsumerBacklog(tasks: StuckTask[]): boolean {
  if (tasks.length === 0) return false;
  const queueKey = noConsumerBacklogQueueGroupKey(tasks[0]);
  return Boolean(
    queueKey &&
    tasks.every((task) => noConsumerBacklogQueueGroupKey(task) === queueKey),
  );
}

function stuckTaskGroupIsQueueBacklog(tasks: StuckTask[]): boolean {
  return tasks.length > 1 && stuckTaskGroupIsNoConsumerBacklog(tasks);
}

function uniqueResourceLabels(tasks: StuckTask[]): string[] {
  const labelsByKey = new Map<string, string>();
  for (const task of tasks) {
    const key = stuckTaskResourceKey(task);
    if (!labelsByKey.has(key)) labelsByKey.set(key, stuckTaskResourceLabel(task));
  }
  return [...labelsByKey.values()];
}

export function stuckTaskAffectedResourcesText(
  tasks: StuckTask[],
  maxVisible = 4,
): string | null {
  if (!stuckTaskGroupIsQueueBacklog(tasks)) return null;
  const labels = uniqueResourceLabels(tasks);
  if (labels.length === 0) return null;

  const visible = labels.slice(0, maxVisible);
  const suffix = labels.length > visible.length
    ? `, +${labels.length - visible.length} more`
    : "";
  return `Shown resources: ${visible.join(", ")}${suffix}`;
}

export function stuckTaskGroupOccurrenceLabel(tasks: StuckTask[]): string | null {
  if (tasks.length <= 1) return null;
  if (stuckTaskGroupIsQueueBacklog(tasks)) {
    const affectedCount = uniqueResourceLabels(tasks).length;
    return affectedCount === 1
      ? "1 shown resource"
      : `${affectedCount} shown resources`;
  }
  const streams = new Set(
    tasks
      .map((task) => task.stream?.trim())
      .filter((stream): stream is string => Boolean(stream)),
  );
  if (streams.size > 1 && streams.size === tasks.length) {
    return `${streams.size} queues`;
  }
  if (streams.size > 1) {
    return `${tasks.length} occurrences across ${streams.size} queues`;
  }
  return `${tasks.length} occurrences`;
}

export interface StuckTaskQueueSummary {
  stream: string;
  label: string;
  count: number;
  noWorkerConsumers: number;
  backlogCount: number;
  backlogExact: boolean;
}

export function stuckTaskQueueSummaries(
  tasks: StuckTask[],
  consumerGroups: ConsumerGroupInfo[] = [],
  queueDepths: Record<string, number> = {},
  queuedTasks: QueuedTask[] = [],
): StuckTaskQueueSummary[] {
  const summaries = new Map<string, StuckTaskQueueSummary>();
  for (const task of tasks) {
    const stream = task.stream?.trim();
    if (!stream) continue;
    const current = summaries.get(stream) ?? {
      stream,
      label: redisStreamDisplayName(stream),
      count: 0,
      noWorkerConsumers: 0,
      backlogCount: 0,
      backlogExact: true,
    };
    current.count += 1;
    if (task.no_worker_consumers) current.noWorkerConsumers += 1;
    summaries.set(stream, current);
  }

  const queuedPreviewsByStream = queuedPreviewCountsByStream(queuedTasks);
  for (const summary of summaries.values()) {
    const groups = consumerGroups.filter((group) => consumerGroupStreamKey(group) === summary.stream);
    for (const group of groups) {
      const backlog = consumerGroupBacklogEvidence(group, {
        queueDepth: consumerGroupQueueDepth(group, queueDepths),
        queuedPreview: consumerGroupQueuedPreviewCount(group, queuedPreviewsByStream),
      });
      if (backlog.count <= summary.backlogCount) continue;
      summary.backlogCount = backlog.count;
      summary.backlogExact = backlog.exact;
    }
  }

  return [...summaries.values()].sort((a, b) =>
    Math.max(b.count, b.backlogCount) - Math.max(a.count, a.backlogCount) ||
      a.label.localeCompare(b.label)
  );
}

export function stuckTaskQueueSummaryText(summary: StuckTaskQueueSummary): string {
  const countLabel = summary.count === 1 ? "1 task" : `${summary.count} tasks`;
  const visibleCountLabel = summary.count === 1 ? "1 task shown" : `${summary.count} tasks shown`;
  const backlogCount = Math.max(0, summary.backlogCount);
  const backlogLabel = backlogCount > summary.count
    ? `${summary.backlogExact ? "" : "at least "}${backlogCount} queued/pending, ${visibleCountLabel}`
    : countLabel;
  const suffix = summary.noWorkerConsumers === summary.count
    ? ", no consumers"
    : summary.noWorkerConsumers > 0
      ? `, ${summary.noWorkerConsumers} no-consumer`
      : "";
  return `${summary.label}: ${backlogLabel}${suffix}`;
}

function comparable(value: string | null | undefined): string {
  return (value || "").trim().toLowerCase();
}

function resourcePrefixesCompatible(
  expectedPrefix: string | null | undefined,
  candidatePrefix: string | null | undefined,
): boolean {
  const expected = comparable(expectedPrefix);
  const candidate = comparable(candidatePrefix);
  if (expected && candidate) return expected === candidate;
  if (expected && !candidate) return true;
  if (!expected && candidate) return false;
  return true;
}

function queuedTaskMatchesStuck(task: StuckTask, queued: QueuedTask): boolean {
  if (!resourcePrefixesCompatible(task.prefix, queued.prefix ?? null)) return false;
  if (comparable(task.resource_type) !== comparable(queued.resource_type)) return false;
  if (comparable(task.resource_id) !== comparable(queued.resource_id)) return false;
  const taskRepo = comparable(task.repo);
  if (taskRepo && taskRepo !== comparable(queued.repo)) return false;
  return true;
}

export function stuckTaskQueueContextText(
  task: StuckTask,
  queuedTasks: QueuedTask[] = [],
  consumerGroups: ConsumerGroupInfo[] = [],
  queueDepths: Record<string, number> = {},
): string | null {
  const queuedPreviewsByStream = queuedPreviewCountsByStream(queuedTasks);
  const noConsumerStreams = new Set(
    consumerGroups
      .filter((group) => consumerGroupHasNoConsumersWhileBacklogged(
        group,
        consumerGroupQueueDepth(group, queueDepths),
        consumerGroupQueuedPreviewCount(group, queuedPreviewsByStream),
      ))
      .map(consumerGroupStreamKey),
  );
  const directStream = task.stream?.trim();
  if (directStream) {
    const suffix = task.no_worker_consumers || noConsumerStreams.has(directStream)
      ? " with no worker consumers"
      : "";
    const ids = [
      task.entry_id ? `entry ${task.entry_id}` : "",
      task.task_id ? `task ${task.task_id}` : "",
    ].filter(Boolean);
    return [
      `Queued in ${redisStreamDisplayName(directStream)}${suffix}`,
      ...ids,
    ].join("; ");
  }

  const streams = [
    ...new Set(
      queuedTasks
        .filter((queued) => queuedTaskMatchesStuck(task, queued))
        .map((queued) => queued.stream.trim())
        .filter(Boolean),
    ),
  ];
  if (streams.length === 0) return null;

  if (streams.length === 1) {
    const stream = streams[0];
    const suffix = noConsumerStreams.has(stream)
      ? " with no worker consumers"
      : "";
    return `Queued in ${redisStreamDisplayName(stream)}${suffix}`;
  }

  const noConsumerCount = streams.filter((stream) => noConsumerStreams.has(stream)).length;
  return noConsumerCount > 0
    ? `Queued in ${streams.length} streams; ${noConsumerCount} have no worker consumers`
    : `Queued in ${streams.length} streams`;
}

function stuckTaskGroupContextLines(
  tasks: StuckTask[],
  queuedTasks: QueuedTask[],
  consumerGroups: ConsumerGroupInfo[],
  queueDepths: Record<string, number>,
): string[] {
  return [
    ...new Set(
      tasks
        .map((task) => stuckTaskQueueContextText(task, queuedTasks, consumerGroups, queueDepths))
        .filter((context): context is string => Boolean(context)),
    ),
  ];
}

function stuckTaskListItem(
  tasks: StuckTask[],
  occurrence: number,
  queuedTasks: QueuedTask[],
  consumerGroups: ConsumerGroupInfo[],
  queueDepths: Record<string, number>,
  contextClassName: string,
) {
  const task = tasks[0];
  const occurrenceLabel = stuckTaskGroupOccurrenceLabel(tasks);
  const queueContexts = stuckTaskGroupContextLines(tasks, queuedTasks, consumerGroups, queueDepths);
  const queueAggregateStream = stuckTaskGroupIsQueueBacklog(tasks)
    ? task.stream?.trim()
    : null;
  const affectedResourcesText = stuckTaskAffectedResourcesText(tasks);
  return (
    <li key={stuckTaskAlertKey(task, occurrence)} className="break-words">
      <div>
        {queueAggregateStream
          ? `Queue ${redisStreamDisplayName(queueAggregateStream)}`
          : stuckTaskResourceLabel(task)}: {stuckTaskReasonText(task.reason)}
        {occurrenceLabel && (
          <span className={`ml-2 whitespace-nowrap rounded-full px-2 py-0.5 text-xs ${contextClassName}`}>
            {occurrenceLabel}
          </span>
        )}
      </div>
      {affectedResourcesText && (
        <div className={`mt-0.5 text-xs ${contextClassName}`}>
          {affectedResourcesText}
        </div>
      )}
      {queueContexts.length > 0 && (
        <div className={`mt-0.5 space-y-0.5 text-xs ${contextClassName}`}>
          {queueContexts.map((queueContext) => (
            <div key={queueContext}>{queueContext}</div>
          ))}
        </div>
      )}
    </li>
  );
}

function StuckQueueSummaryRow({
  summaries,
  contextClassName,
}: {
  summaries: StuckTaskQueueSummary[];
  contextClassName: string;
}) {
  if (summaries.length === 0) return null;
  const visible = summaries.slice(0, 4);
  const hiddenCount = summaries.length - visible.length;
  return (
    <div className={`mb-2 flex flex-wrap items-center gap-1 text-xs ${contextClassName}`}>
      <span className="font-medium">Queues:</span>
      {visible.map((summary) => (
        <span
          key={summary.stream}
          className="rounded-full border border-current/20 px-2 py-0.5"
          title={summary.stream}
        >
          {stuckTaskQueueSummaryText(summary)}
        </span>
      ))}
      {hiddenCount > 0 && (
        <span className="rounded-full border border-current/20 px-2 py-0.5">
          +{hiddenCount} more
        </span>
      )}
    </div>
  );
}

export function StuckAlerts({
  stuckTasks,
  queuedTasks = [],
  consumerGroups = [],
  queueDepths = {},
  degraded = false,
}: Props) {
  if (stuckTasks.length === 0) {
    return degraded ? (
      <div
        role="alert"
        className="rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-4 py-3 text-sm text-yellow-200"
      >
        Stuck task detection is partially unavailable
      </div>
    ) : null;
  }

  const critical = stuckTasks.filter((t) => t.severity === "critical");
  const warnings = stuckTasks.filter((t) => t.severity === "warning");
  const criticalGroups = groupedStuckTasks(critical);
  const warningGroups = groupedStuckTasks(warnings);
  const criticalQueueSummaries = stuckTaskQueueSummaries(
    critical,
    consumerGroups,
    queueDepths,
    queuedTasks,
  );
  const warningQueueSummaries = stuckTaskQueueSummaries(
    warnings,
    consumerGroups,
    queueDepths,
    queuedTasks,
  );

  return (
    <div className="space-y-2">
      {degraded && (
        <div
          role="alert"
          className="rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-4 py-3 text-sm text-yellow-200"
        >
          Stuck task detection is partially unavailable
        </div>
      )}
      {critical.length > 0 && (
        <div
          role="alert"
          className="rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3"
        >
          <div className="flex items-center gap-2 mb-1">
            <span className="text-red-400 font-medium">
              {stuckTaskSeverityGroupsLabel(criticalGroups, "critical", criticalQueueSummaries)}
            </span>
          </div>
          <StuckQueueSummaryRow
            summaries={criticalQueueSummaries}
            contextClassName="text-red-200/80"
          />
          <ul className="text-sm text-red-300 space-y-1">
            {criticalGroups.map((tasks, i) =>
              stuckTaskListItem(tasks, i, queuedTasks, consumerGroups, queueDepths, "text-red-200/70")
            )}
          </ul>
        </div>
      )}
      {warnings.length > 0 && (
        <div
          role="status"
          className="rounded-lg border border-yellow-500/30 bg-yellow-500/10 px-4 py-3"
        >
          <div className="flex items-center gap-2 mb-1">
            <span className="text-yellow-400 font-medium">
              {stuckTaskSeverityGroupsLabel(warningGroups, "warning", warningQueueSummaries)}
            </span>
          </div>
          <StuckQueueSummaryRow
            summaries={warningQueueSummaries}
            contextClassName="text-yellow-200/80"
          />
          <ul className="text-sm text-yellow-300 space-y-1">
            {warningGroups.map((tasks, i) =>
              stuckTaskListItem(tasks, i, queuedTasks, consumerGroups, queueDepths, "text-yellow-200/70")
            )}
          </ul>
        </div>
      )}
    </div>
  );
}
