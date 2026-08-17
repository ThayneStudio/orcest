import { useEffect, useMemo, useRef, useState, type RefCallback } from "react";
import type { QueuedTask, RecentResult, SystemSnapshot, StuckTask, TaskOutputParams } from "../lib/types";
import { formatDuration, formatTimestampMs, formatTtl, statusColor, timeAgo } from "../lib/format";
import { degradedCountLabel, loadedCountLabel } from "../lib/counts";
import { deferFocus } from "../lib/focus";
import {
  taskOutputControlLabel,
  taskOutputDomId,
  taskOutputParamsEqual,
  taskOutputParamsForLock,
  taskOutputParamsForResult,
  taskOutputParamsInstanceEqual,
  taskOutputParamsTargetEqual,
  taskOutputSelectionStillVisible,
} from "../lib/taskOutput";
import { lockMatchesStuck, resourceKey, resourceLabel, stuckTaskKeys } from "../lib/resource";
import {
  partitionResultsByStatus,
  resultEntryIdParts,
  resultResourceLabel,
  resultStatusLabel,
  resultTimestampMs,
} from "../lib/results";
import { normalizeQueueDepths, redisStreamDisplayName } from "../lib/queues";
import {
  consumerGroupBacklogEvidence,
  consumerGroupHasNoConsumersWhileBacklogged,
  consumerGroupQueueDepth,
  consumerGroupQueuedPreviewCount,
  consumerGroupStreamKey,
  isWorkerConsumerGroup,
  queuedPreviewCountsByStream,
} from "../lib/consumerGroups";
import { hasDegradedSection, normalizedDegradedSectionSet } from "../lib/degradedSections";
import { TaskOutputPanel } from "./TaskOutputPanel";

interface Props {
  snapshot: SystemSnapshot;
  stuckTasks: StuckTask[];
}

export const KANBAN_RESULT_COLUMN_LIMIT = 20;

export function kanbanColumnsPaneClassName(hasSelectedTask: boolean): string {
  return `flex gap-4 overflow-x-auto min-h-0 ${
    hasSelectedTask ? "flex-[1_1_0] basis-0" : "flex-1"
  }`;
}

export const KANBAN_OUTPUT_PANEL_CLASS =
  "min-h-0 flex-[1_1_0] basis-0 border-t border-zinc-800 pt-3";
export const KANBAN_BOARD_CLASS =
  "flex min-h-0 flex-1 flex-col rounded-sm focus:outline-none focus:ring-2 focus:ring-zinc-500";

function taskTypeLabel(type: string): string {
  switch (type.toLowerCase()) {
    case "fix_pr": return "Fix PR";
    case "fix_ci": return "Fix CI";
    case "classify_ci": return "Classify CI";
    case "implement_issue": return "Implement";
    case "triage_followups": return "Triage";
    case "rebase_pr": return "Rebase";
    case "improve": return "Improve";
    default: return type;
  }
}

function taskTypeBadge(type: string): string {
  switch (type.toLowerCase()) {
    case "fix_pr":
    case "fix_ci": return "bg-orange-500/20 text-orange-400";
    case "classify_ci": return "bg-amber-500/20 text-amber-400";
    case "implement_issue": return "bg-blue-500/20 text-blue-400";
    case "triage_followups": return "bg-violet-500/20 text-violet-400";
    case "rebase_pr": return "bg-cyan-500/20 text-cyan-400";
    default: return "bg-zinc-500/20 text-zinc-400";
  }
}

export function queuedTaskKey(task: QueuedTask): string {
  return JSON.stringify([
    task.stream,
    task.entry_id || "(missing-entry-id)",
    task.task_id || "(missing-task-id)",
    task.prefix || "(missing-prefix)",
    task.repo,
    task.resource_type,
    task.resource_id,
    task.created_at || "(unknown-created-at)",
  ]);
}

export function queuedResourceKey(
  task: Pick<QueuedTask, "prefix" | "repo" | "resource_type" | "resource_id">,
): string {
  return resourceKey(task.resource_type, task.repo, task.resource_id, task.prefix ?? null);
}

export function queuedResourceStreamCounts(tasks: QueuedTask[]): Map<string, number> {
  const streamsByResource = new Map<string, Set<string>>();
  for (const task of tasks) {
    const key = queuedResourceKey(task);
    const streams = streamsByResource.get(key) ?? new Set<string>();
    const stream = task.stream.trim();
    if (stream) streams.add(stream);
    streamsByResource.set(key, streams);
  }

  return new Map(
    Array.from(streamsByResource.entries(), ([key, streams]) => [key, streams.size]),
  );
}

export function queuedResourceOccurrenceCounts(tasks: QueuedTask[]): Map<string, number> {
  const counts = new Map<string, number>();
  for (const task of tasks) {
    const key = queuedResourceKey(task);
    counts.set(key, (counts.get(key) || 0) + 1);
  }
  return counts;
}

export function queuedResourceDuplicateLabel(
  streamCount: number,
  occurrenceCount = 1,
): string | null {
  const streams = Math.max(0, streamCount);
  const occurrences = Math.max(0, occurrenceCount);
  if (occurrences > 1 && streams > 1) return `${occurrences} entries in ${streams} streams`;
  if (occurrences > 1) return `${occurrences} entries`;
  if (streams > 1) return `${streams} streams`;
  return null;
}

export function queuedResourceDuplicateTitle(label: string): string {
  return /^\d+ streams$/.test(label)
    ? `This resource is queued in ${label}`
    : `This resource is queued as ${label}`;
}

export interface QueuedResourcePreviewRow {
  key: string;
  task: QueuedTask;
  tasks: QueuedTask[];
  occurrenceCount: number;
  streamCount: number;
  streams: string[];
}

export interface QueuedConsumerBacklogRow {
  stream: string;
  depth: number;
}

function queuedTaskSortTimestampMs(task: Pick<QueuedTask, "created_at" | "entry_id">): number {
  const createdAtMs = Date.parse(task.created_at || "");
  if (Number.isFinite(createdAtMs)) return createdAtMs;
  return resultTimestampMs(task.entry_id) ?? Number.POSITIVE_INFINITY;
}

function queuedTaskEntryIdCompare(a: string, b: string): number {
  const aParts = resultEntryIdParts(a);
  const bParts = resultEntryIdParts(b);
  if (aParts && bParts) return aParts[0] - bParts[0] || aParts[1] - bParts[1];
  if (aParts) return -1;
  if (bParts) return 1;
  return a.localeCompare(b);
}

function queuedTaskSortCompare(a: QueuedTask, b: QueuedTask): number {
  const aTimestamp = queuedTaskSortTimestampMs(a);
  const bTimestamp = queuedTaskSortTimestampMs(b);
  if (aTimestamp !== bTimestamp) return aTimestamp - bTimestamp;
  return queuedTaskEntryIdCompare(a.entry_id, b.entry_id) || queuedTaskKey(a).localeCompare(queuedTaskKey(b));
}

function earlierQueuedTask(a: QueuedTask, b: QueuedTask): QueuedTask {
  return queuedTaskSortCompare(a, b) <= 0 ? a : b;
}

export function queuedResourcePreviewRows(tasks: QueuedTask[]): QueuedResourcePreviewRow[] {
  const rows = new Map<string, QueuedResourcePreviewRow>();
  for (const task of tasks) {
    const key = queuedResourceKey(task);
    const existing = rows.get(key);
    if (!existing) {
      rows.set(key, {
        key,
        task,
        tasks: [task],
        occurrenceCount: 1,
        streamCount: task.stream.trim() ? 1 : 0,
        streams: task.stream.trim() ? [task.stream.trim()] : [],
      });
      continue;
    }

    existing.tasks.push(task);
    existing.occurrenceCount += 1;
    existing.task = earlierQueuedTask(existing.task, task);
    const stream = task.stream.trim();
    if (stream && !existing.streams.includes(stream)) {
      existing.streams.push(stream);
      existing.streamCount = existing.streams.length;
    }
  }

  return [...rows.values()].sort((a, b) =>
    queuedTaskSortCompare(a.task, b.task) || a.key.localeCompare(b.key)
  );
}

function queuedResourceTaskTypeLabels(row: Pick<QueuedResourcePreviewRow, "task" | "tasks">): string[] {
  const labels: string[] = [];
  const seen = new Set<string>();
  const tasks = row.tasks.length > 0 ? row.tasks : [row.task];

  for (const task of tasks) {
    const label = taskTypeLabel(task.task_type.trim() || row.task.task_type);
    const key = label.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    labels.push(label);
  }

  return labels;
}

export function queuedResourceTaskTypeLabel(row: Pick<QueuedResourcePreviewRow, "task" | "tasks">): string {
  const labels = queuedResourceTaskTypeLabels(row);
  if (labels.length <= 1) return labels[0] ?? taskTypeLabel(row.task.task_type);
  return "Mixed types";
}

export function queuedResourceTaskTypeTitle(row: Pick<QueuedResourcePreviewRow, "task" | "tasks">): string | undefined {
  const labels = queuedResourceTaskTypeLabels(row);
  return labels.length > 1 ? `Queued task types: ${labels.join(", ")}` : undefined;
}

export function queuedResourceTaskTypeBadge(row: Pick<QueuedResourcePreviewRow, "task" | "tasks">): string {
  return queuedResourceTaskTypeLabels(row).length > 1
    ? "bg-yellow-500/15 text-yellow-300"
    : taskTypeBadge(row.task.task_type);
}

export function queuedResourceStreamDisplay(row: Pick<QueuedResourcePreviewRow, "streamCount" | "streams" | "task">): string {
  if (row.streamCount > 1) return `${row.streamCount} streams`;
  const stream = row.streams[0] || row.task.stream;
  return redisStreamDisplayName(stream);
}

export function queuedResourceStreamTitle(row: Pick<QueuedResourcePreviewRow, "streamCount" | "streams" | "task">): string {
  if (row.streamCount > 1) return row.streams.join(", ");
  return row.streams[0] || row.task.stream;
}

export function queuedResourceNoConsumerStreams(
  row: Pick<QueuedResourcePreviewRow, "tasks">,
  noConsumerQueuedStreams: ReadonlySet<string>,
): string[] {
  const streams: string[] = [];
  for (const task of row.tasks) {
    const stream = task.stream.trim();
    if (!stream || !noConsumerQueuedStreams.has(stream) || streams.includes(stream)) continue;
    streams.push(stream);
  }
  return streams;
}

export function queuedResourceNoConsumerTitle(streams: string[]): string {
  if (streams.length === 0) return "No consumers for queued resource";
  return `No consumers for ${streams.map(redisStreamDisplayName).join(", ")}`;
}

export function kanbanQueuedGroupingMessage(resourceCount: number, entryCount: number): string | null {
  if (resourceCount >= entryCount) return null;
  return `Showing ${resourceCount} queued resources from ${entryCount} queued entries`;
}

type QueuedBacklogSnapshot = Pick<SystemSnapshot, "queue_depths" | "queued_tasks"> &
  Partial<Pick<SystemSnapshot, "consumer_groups">>;

function queuedBacklogCountsByStream(snapshot: QueuedBacklogSnapshot): Map<string, number> {
  const counts = new Map<string, number>();
  const setMax = (stream: string, depth: number) => {
    const name = stream.trim();
    if (!name || depth <= 0) return;
    counts.set(name, Math.max(counts.get(name) || 0, depth));
  };

  for (const queue of normalizeQueueDepths(snapshot.queue_depths)) {
    setMax(queue.name, queue.depth);
  }

  const queuedPreviewsByStream = queuedPreviewCountsByStream(snapshot.queued_tasks);
  for (const [stream, depth] of Object.entries(queuedPreviewsByStream)) {
    setMax(stream, depth ?? 0);
  }

  for (const group of snapshot.consumer_groups ?? []) {
    const stream = consumerGroupStreamKey(group);
    const queuedPreview = consumerGroupQueuedPreviewCount(group, queuedPreviewsByStream);
    const queueDepth = consumerGroupQueueDepth(group, snapshot.queue_depths);
    const backlog = consumerGroupBacklogEvidence(group, {
      queueDepth,
      queuedPreview,
    });
    if (!isWorkerConsumerGroup(group) || backlog.count === 0) continue;
    setMax(stream, backlog.count);
  }

  return counts;
}

export function queuedConsumerBacklogRows(
  snapshot: Pick<SystemSnapshot, "consumer_groups" | "queue_depths" | "queued_tasks">,
): QueuedConsumerBacklogRow[] {
  const queuedPreviewsByStream = queuedPreviewCountsByStream(snapshot.queued_tasks);
  const rowsByStream = new Map<string, QueuedConsumerBacklogRow>();

  for (const group of snapshot.consumer_groups) {
    const stream = consumerGroupStreamKey(group);
    const queuedPreview = consumerGroupQueuedPreviewCount(group, queuedPreviewsByStream);
    const queueDepth = consumerGroupQueueDepth(group, snapshot.queue_depths);
    if (!stream || !consumerGroupHasNoConsumersWhileBacklogged(group, queueDepth, queuedPreview)) continue;
    const depth = consumerGroupBacklogEvidence(group, {
      queueDepth,
      queuedPreview,
    }).count;
    const existing = rowsByStream.get(stream);
    rowsByStream.set(stream, {
      stream,
      depth: Math.max(existing?.depth || 0, depth),
    });
  }

  return [...rowsByStream.values()].sort((a, b) => b.depth - a.depth || a.stream.localeCompare(b.stream));
}

export function kanbanConsumerBacklogRowLabel(row: QueuedConsumerBacklogRow): string {
  const itemLabel = row.depth === 1 ? "item" : "items";
  return `${redisStreamDisplayName(row.stream)} has no consumers; at least ${row.depth} queued/pending ${itemLabel}`;
}

export function kanbanQueuedTimestampText(task: Pick<QueuedTask, "created_at" | "entry_id">): string | null {
  const createdText = timeAgo(task.created_at);
  if (createdText) return createdText;
  const timestampMs = resultTimestampMs(task.entry_id);
  if (timestampMs === null) return null;
  return timeAgo(new Date(timestampMs).toISOString()) || formatTimestampMs(timestampMs);
}

export function queuedColumnCount(
  snapshot: QueuedBacklogSnapshot,
): number {
  const backlogTotal = [...queuedBacklogCountsByStream(snapshot).values()]
    .reduce((total, depth) => total + depth, 0);
  return Math.max(backlogTotal, snapshot.queued_tasks.length);
}

export type QueuedPreviewState = "empty" | "complete" | "partial" | "unavailable";

export function queuedPreviewState(
  totalQueued: number,
  previewCount: number,
  degraded = false,
): QueuedPreviewState {
  if (previewCount === 0) return degraded || totalQueued > 0 ? "unavailable" : "empty";
  return degraded || previewCount < totalQueued ? "partial" : "complete";
}

export function kanbanBacklogEmptyMessage(degraded: boolean): string {
  return degraded ? "Queued/pending work unavailable" : "No queued or pending work";
}

export function kanbanBacklogUnavailableMessage(queueDepthsDegraded: boolean): string {
  return queueDepthsDegraded
    ? "Queued/pending work unavailable"
    : "Queued/pending work reported; task preview unavailable";
}

export function kanbanBacklogPartialMessage(
  previewCount: number,
  totalQueued: number,
  degraded: boolean,
): string {
  return degraded
    ? "Queued/pending preview may be incomplete"
    : `Showing ${previewCount} queued previews; ${totalQueued} queued/pending total`;
}

export type ResultColumnKind = "completed" | "failed" | "neutral";

export function kanbanResultEmptyMessage(kind: ResultColumnKind, degraded: boolean): string {
  if (degraded) {
    switch (kind) {
      case "completed": return "No completions in loaded results";
      case "failed": return "No loaded results need attention";
      case "neutral": return "No skipped results in loaded results";
    }
  }

  switch (kind) {
    case "completed": return "No recent completions";
    case "failed": return "No results need attention";
    case "neutral": return "No skipped results";
  }
}

export function kanbanActiveEmptyMessage(degraded: boolean): string {
  return degraded ? "Active work unavailable" : "No active work";
}

export function kanbanTaskIdDisplay(taskId: string | null | undefined, maxLength = 12): string | null {
  const trimmed = taskId?.trim();
  if (!trimmed) return null;
  return trimmed.length <= maxLength ? trimmed : `${trimmed.slice(0, maxLength)}...`;
}

export function kanbanTaskOutputPanelKey(params: TaskOutputParams): string {
  return taskOutputDomId(params, "task-output-panel");
}

export function kanbanResultTimestampText(
  result: Pick<RecentResult, "entry_id">,
): string {
  return formatTimestampMs(resultTimestampMs(result.entry_id));
}

export function kanbanColumnTitleId(title: string): string {
  return `kanban-column-${title.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/^-|-$/g, "")}`;
}

export function kanbanResultLimitMessage(
  total: number,
  degraded: boolean,
  limit = KANBAN_RESULT_COLUMN_LIMIT,
): string | null {
  if (total <= limit) return null;
  const source = degraded ? "loaded results" : "results";
  return `Showing ${limit} of ${total} ${source}`;
}

export function kanbanResultColumnCountLabel(
  count: number,
  loadedPreviewOnly = false,
  degraded = false,
): string {
  return loadedPreviewOnly ? loadedCountLabel(count, degraded) : degradedCountLabel(count, degraded);
}

export function kanbanResultPreviewMessage(
  loaded: number,
  total = loaded,
  entriesDegraded = false,
  depthDegraded = false,
): string | null {
  const normalizedLoaded = Math.max(0, loaded);
  const normalizedTotal = Math.max(normalizedLoaded, total);
  if (depthDegraded) {
    return normalizedLoaded > 0
      ? `Result columns show ${normalizedLoaded} loaded results; total unavailable`
      : "Result columns total unavailable";
  }
  if (entriesDegraded) {
    return normalizedTotal > normalizedLoaded
      ? `Result columns may be incomplete; showing ${normalizedLoaded} loaded of ${normalizedTotal} retained results`
      : `Result columns may be incomplete; showing ${normalizedLoaded} loaded results`;
  }
  return normalizedTotal > normalizedLoaded
    ? `Result columns show ${normalizedLoaded} loaded of ${normalizedTotal} retained results`
    : null;
}

export function kanbanVisibleRecentResults(
  results: RecentResult[],
  limit = KANBAN_RESULT_COLUMN_LIMIT,
): RecentResult[] {
  const { completed, failed, neutral } = partitionResultsByStatus(results);
  return [
    ...completed.slice(0, limit),
    ...failed.slice(0, limit),
    ...neutral.slice(0, limit),
  ];
}

export function kanbanOutputSelectionStillVisible(
  selected: TaskOutputParams | null,
  snapshot: Pick<SystemSnapshot, "locks" | "recent_results">,
  sections: { activeLocksDegraded: boolean; recentResultsDegraded: boolean },
): boolean {
  if (!selected) return false;
  if (selected.historical) {
    return taskOutputSelectionStillVisible(
      selected,
      kanbanVisibleRecentResults(snapshot.recent_results).map(taskOutputParamsForResult),
      sections.recentResultsDegraded,
    );
  }

  return taskOutputSelectionStillVisible(
    selected,
    snapshot.locks.map(taskOutputParamsForLock),
    sections.activeLocksDegraded,
  );
}

export function Kanban({ snapshot, stuckTasks }: Props) {
  const stuckKeys = stuckTaskKeys(stuckTasks);
  const [selectedTask, setSelectedTask] = useState<TaskOutputParams | null>(null);
  const [selectedLabel, setSelectedLabel] = useState<string>("");
  const [autoFocusSelectedTask, setAutoFocusSelectedTask] = useState(false);
  const boardRef = useRef<HTMLDivElement>(null);
  const outputTriggerRefs = useRef(new Map<string, HTMLButtonElement>());
  const focusedOutputTriggerIdRef = useRef<string | null>(null);
  const degradedSections = normalizedDegradedSectionSet(snapshot.degraded_sections);
  const isDegraded = (section: string) => hasDegradedSection(degradedSections, section);
  const queueDepthsDegraded = isDegraded("queue depths");
  const queuedTasksDegraded = isDegraded("queued tasks");
  const queuedDataDegraded =
    queuedTasksDegraded || queueDepthsDegraded;
  const activeLocksDegraded = isDegraded("active locks");
  const recentResultsDegraded = isDegraded("recent results");
  const resultDepthDegraded = isDegraded("results depth");
  const resultCountsArePreview =
    recentResultsDegraded ||
    resultDepthDegraded ||
    snapshot.results_depth > snapshot.recent_results.length;
  const resultPreviewMessage = kanbanResultPreviewMessage(
    snapshot.recent_results.length,
    snapshot.results_depth,
    recentResultsDegraded,
    resultDepthDegraded,
  );
  const visibleRecentResults = useMemo(
    () => kanbanVisibleRecentResults(snapshot.recent_results),
    [snapshot.recent_results],
  );

  const { completed, failed, neutral } = partitionResultsByStatus(snapshot.recent_results);
  const queuedCount = queuedColumnCount(snapshot);
  const queuedState = queuedPreviewState(queuedCount, snapshot.queued_tasks.length, queuedDataDegraded);
  const queuedResourceStreamCountsByKey = queuedResourceStreamCounts(snapshot.queued_tasks);
  const queuedResourceOccurrenceCountsByKey = queuedResourceOccurrenceCounts(snapshot.queued_tasks);
  const queuedResourceRows = queuedResourcePreviewRows(snapshot.queued_tasks);
  const queuedGroupingMessage = kanbanQueuedGroupingMessage(
    queuedResourceRows.length,
    snapshot.queued_tasks.length,
  );
  const queuedPreviewsByStream = queuedPreviewCountsByStream(snapshot.queued_tasks);
  const noConsumerQueuedStreams = new Set(
    snapshot.consumer_groups
      .filter((group) => consumerGroupHasNoConsumersWhileBacklogged(
        group,
        consumerGroupQueueDepth(group, snapshot.queue_depths),
        consumerGroupQueuedPreviewCount(group, queuedPreviewsByStream),
      ))
      .map(consumerGroupStreamKey)
      .filter(Boolean),
  );
  const queuedPreviewStreamNames = new Set(Object.keys(queuedPreviewsByStream));
  const noConsumerBacklogRowsWithoutPreview = queuedConsumerBacklogRows(snapshot)
    .filter((row) => !queuedPreviewStreamNames.has(row.stream));

  const selectedTaskContainsFocus = () => {
    if (!selectedTask || typeof document === "undefined") return false;
    const triggerId = taskOutputDomId(selectedTask);
    const trigger = outputTriggerRefs.current.get(triggerId);
    const panel = document.getElementById(triggerId);
    const active = document.activeElement;
    return Boolean(
      (panel && active && panel.contains(active)) ||
      (trigger && active === trigger) ||
      focusedOutputTriggerIdRef.current === triggerId
    );
  };

  useEffect(() => {
    if (!selectedTask) return;
    const candidates = selectedTask.historical
      ? visibleRecentResults.map((result) => ({
        params: taskOutputParamsForResult(result),
        label: resultResourceLabel(result),
      }))
      : snapshot.locks.map((lock) => ({
        params: taskOutputParamsForLock(lock),
        label: resourceLabel(lock),
      }));
    const exactMatch = candidates.find((candidate) =>
      taskOutputParamsEqual(selectedTask, candidate.params)
    );
    const instanceMatch = exactMatch
      ? undefined
      : candidates.find((candidate) =>
        taskOutputParamsInstanceEqual(selectedTask, candidate.params)
      );
    const targetMatches = exactMatch || instanceMatch
      ? []
      : candidates.filter((candidate) =>
        taskOutputParamsTargetEqual(selectedTask, candidate.params)
      );
    const match =
      exactMatch ||
      instanceMatch ||
      (targetMatches.length === 1 ? targetMatches[0] : undefined);
    if (match) {
      if (!taskOutputParamsEqual(selectedTask, match.params)) {
        setAutoFocusSelectedTask(selectedTaskContainsFocus());
        setSelectedTask(match.params);
      }
      setSelectedLabel((current) => current === match.label ? current : match.label);
      return;
    }
    if (!kanbanOutputSelectionStillVisible(selectedTask, snapshot, {
      activeLocksDegraded,
      recentResultsDegraded,
    })) {
      closeSelectedTask(selectedTaskContainsFocus());
    }
  }, [activeLocksDegraded, recentResultsDegraded, snapshot, selectedTask, visibleRecentResults]);

  const columns = [
    { title: "Queued / Pending", color: "border-zinc-500", headerBg: "bg-zinc-800", count: degradedCountLabel(queuedCount, queuedDataDegraded) },
    { title: "In Progress", color: "border-blue-500", headerBg: "bg-blue-500/10", count: degradedCountLabel(snapshot.locks.length, activeLocksDegraded) },
    { title: "Completed", color: "border-emerald-500", headerBg: "bg-emerald-500/10", count: kanbanResultColumnCountLabel(completed.length, resultCountsArePreview, recentResultsDegraded) },
    { title: "Needs Attention", color: "border-red-500", headerBg: "bg-red-500/10", count: kanbanResultColumnCountLabel(failed.length, resultCountsArePreview, recentResultsDegraded) },
    { title: "Skipped / Stale", color: "border-zinc-600", headerBg: "bg-zinc-800/80", count: kanbanResultColumnCountLabel(neutral.length, resultCountsArePreview, recentResultsDegraded) },
  ];

  const trackOutputTrigger = (id: string): RefCallback<HTMLButtonElement> => (button) => {
    if (button) outputTriggerRefs.current.set(id, button);
    else {
      const previous = outputTriggerRefs.current.get(id);
      if (
        previous &&
        typeof document !== "undefined" &&
        document.activeElement === previous
      ) {
        focusedOutputTriggerIdRef.current = id;
      }
      outputTriggerRefs.current.delete(id);
    }
  };

  const rememberFocusedOutputTrigger = (id: string | undefined) => {
    if (id) focusedOutputTriggerIdRef.current = id;
  };

  useEffect(() => {
    if (typeof document === "undefined") return;
    const handleFocusIn = (event: FocusEvent) => {
      const target = event.target;
      if (!(target instanceof Node)) {
        focusedOutputTriggerIdRef.current = null;
        return;
      }
      for (const [id, button] of outputTriggerRefs.current) {
        if (button === target || button.contains(target)) {
          focusedOutputTriggerIdRef.current = id;
          return;
        }
      }
      focusedOutputTriggerIdRef.current = null;
    };

    document.addEventListener("focusin", handleFocusIn);
    return () => document.removeEventListener("focusin", handleFocusIn);
  }, []);

  const closeSelectedTask = (restoreFocus = false) => {
    const task = selectedTask;
    setSelectedTask(null);
    setSelectedLabel("");
    setAutoFocusSelectedTask(false);
    if (restoreFocus && task) {
      const triggerId = taskOutputDomId(task);
      deferFocus(() => outputTriggerRefs.current.get(triggerId) ?? boardRef.current);
    }
  };

  const selectTask = (params: TaskOutputParams | null, label: string) => {
    if (taskOutputParamsEqual(selectedTask, params)) {
      closeSelectedTask();
    } else {
      setAutoFocusSelectedTask(true);
      setSelectedTask(params);
      setSelectedLabel(label);
    }
  };

  return (
    <div
      ref={boardRef}
      tabIndex={-1}
      role="region"
      aria-label="Kanban board"
      className={`${KANBAN_BOARD_CLASS} ${
        selectedTask ? "gap-3" : ""
      }`}
    >
      {resultPreviewMessage && (
        <div
          role="note"
          className={`mb-3 rounded-md border px-3 py-2 text-xs ${
            recentResultsDegraded || resultDepthDegraded
              ? "border-yellow-500/30 bg-yellow-500/10 text-yellow-300"
              : "border-zinc-800 bg-zinc-950 text-zinc-500"
          }`}
        >
          {resultPreviewMessage}
        </div>
      )}
      <div className={kanbanColumnsPaneClassName(Boolean(selectedTask))}>
        {/* Backlog */}
        <Column header={columns[0]}>
          {queuedState === "empty" ? (
            <EmptyState>{kanbanBacklogEmptyMessage(queuedDataDegraded)}</EmptyState>
          ) : (
            <>
              {queuedState === "unavailable" && (
                <EmptyState>{kanbanBacklogUnavailableMessage(queueDepthsDegraded)}</EmptyState>
              )}
              {queuedState === "partial" && (
                <div className="rounded-md border border-zinc-800 bg-zinc-900/70 px-2 py-1.5 text-xs text-zinc-500">
                  {kanbanBacklogPartialMessage(
                    snapshot.queued_tasks.length,
                    queuedCount,
                    queuedDataDegraded,
                  )}
                </div>
              )}
              {noConsumerBacklogRowsWithoutPreview.map((row) => (
                <div
                  key={`no-consumer-backlog:${row.stream}`}
                  className="rounded-md border border-red-500/30 bg-red-500/10 px-2 py-1.5 text-xs text-red-300"
                  title={row.stream}
                >
                  {kanbanConsumerBacklogRowLabel(row)}
                </div>
              ))}
              {queuedGroupingMessage && (
                <div className="rounded-md border border-zinc-800 bg-zinc-900/70 px-2 py-1.5 text-xs text-zinc-500">
                  {queuedGroupingMessage}
                </div>
              )}
              {queuedResourceRows.map((row) => {
                const { task } = row;
                const queuedTimestamp = kanbanQueuedTimestampText(task);
                const noConsumerStreams = queuedResourceNoConsumerStreams(row, noConsumerQueuedStreams);
                const noConsumers = noConsumerStreams.length > 0;
                const queuedResource = row.key;
                const taskTypeTitle = queuedResourceTaskTypeTitle(row);
                const duplicateQueueLabel = queuedResourceDuplicateLabel(
                  queuedResourceStreamCountsByKey.get(queuedResource) ?? 0,
                  queuedResourceOccurrenceCountsByKey.get(queuedResource) ?? 0,
                );
                return (
                  <Card
                    key={row.key}
                    className={`opacity-75 ${noConsumers ? "border-red-500/40" : ""}`}
                  >
                    <div className="flex items-center justify-between mb-2">
                      <span className="min-w-0 truncate font-mono text-sm" title={resourceLabel(task)}>
                        {resourceLabel(task, { includeRepo: false })}
                      </span>
                      <span
                        className={`text-xs rounded-full px-2 py-0.5 ${queuedResourceTaskTypeBadge(row)}`}
                        title={taskTypeTitle}
                      >
                        {queuedResourceTaskTypeLabel(row)}
                      </span>
                    </div>
                    <div className="text-xs text-zinc-500 truncate" title={task.repo}>{task.repo}</div>
                    {(noConsumers || duplicateQueueLabel) && (
                      <div className="mt-2 flex flex-wrap gap-1">
                        {noConsumers && (
                          <span
                            className="inline-flex rounded-full bg-red-500/15 px-2 py-0.5 text-xs text-red-300"
                            title={queuedResourceNoConsumerTitle(noConsumerStreams)}
                          >
                            no consumers
                          </span>
                        )}
                        {duplicateQueueLabel && (
                          <span
                            className="inline-flex rounded-full bg-yellow-500/15 px-2 py-0.5 text-xs text-yellow-300"
                            title={queuedResourceDuplicateTitle(duplicateQueueLabel)}
                          >
                            {duplicateQueueLabel}
                          </span>
                        )}
                      </div>
                    )}
                    <div className="flex items-center justify-between mt-2">
                      <span className="text-xs text-zinc-600 truncate" title={queuedResourceStreamTitle(row)}>
                        <span>{queuedResourceStreamDisplay(row)}</span>
                        <span className="sr-only"> raw stream {queuedResourceStreamTitle(row)}</span>
                      </span>
                      {queuedTimestamp && (
                        <span
                          className="text-xs text-zinc-600"
                          title={task.created_at || formatTimestampMs(resultTimestampMs(task.entry_id))}
                        >
                          {queuedTimestamp}
                        </span>
                      )}
                    </div>
                  </Card>
                );
              })}
            </>
          )}
        </Column>

        {/* In Progress */}
        <Column header={columns[1]}>
          {snapshot.locks.length === 0 ? (
            <EmptyState>{kanbanActiveEmptyMessage(activeLocksDegraded)}</EmptyState>
          ) : (
            snapshot.locks.map((lock) => {
              const isStuck = lockMatchesStuck(lock, stuckKeys);
              const outputParams = taskOutputParamsForLock(lock);
              const isSelected = taskOutputParamsEqual(selectedTask, outputParams);
              const outputPanelId = outputParams ? taskOutputDomId(outputParams) : undefined;
              const label = resourceLabel(lock, { includeRepo: false });
              const fullLabel = resourceLabel(lock);
              const taskId = lock.task_id?.trim() || "";
              const taskIdDisplay = kanbanTaskIdDisplay(taskId);
              const cardDescriptionId = outputPanelId
                ? `${outputPanelId}-card-description`
                : undefined;
              return (
                <Card
                  key={lock.lock_key}
                  className={`transition-colors ${
                    isStuck ? "border-red-500/50 ring-1 ring-red-500/20" :
                    isSelected ? "border-blue-500/50 ring-1 ring-blue-500/30" :
                    outputParams ? "hover:border-zinc-600" : ""
                  } ${outputParams ? "cursor-pointer" : "cursor-default"}`}
                  onClick={outputParams
                    ? () => selectTask(outputParams, fullLabel)
                    : undefined}
                  onFocus={() => rememberFocusedOutputTrigger(outputPanelId)}
                  triggerRef={outputParams && outputPanelId
                    ? trackOutputTrigger(outputPanelId)
                    : undefined}
                  ariaLabel={outputParams
                    ? taskOutputControlLabel(isSelected ? "Hide" : "View", fullLabel, outputParams)
                    : undefined}
                  ariaControls={isSelected ? outputPanelId : undefined}
                  ariaDescribedBy={cardDescriptionId}
                  ariaExpanded={outputParams ? isSelected : undefined}
                  descriptionId={cardDescriptionId}
                >
                  <div className="flex min-w-0 items-center justify-between gap-2 mb-2">
                    <span className="min-w-0 truncate font-mono text-sm">
                      <span title={fullLabel}>{label}</span>
                    </span>
                    <div className="flex shrink-0 items-center gap-1.5">
                      {isStuck && (
                        <span className="text-xs rounded-full px-2 py-0.5 bg-red-500/20 text-red-400">Stuck</span>
                      )}
                      <span className={`text-xs ${outputParams ? "text-blue-400" : "text-zinc-600"}`}>
                        {outputParams ? (isSelected ? "Hide Output" : "View Output") : "No Output"}
                      </span>
                    </div>
                  </div>
                  <div className="text-xs text-zinc-400 truncate" title={lock.repo}>{lock.repo}</div>
                  <div className="truncate text-xs text-zinc-400" title={lock.owner}>
                    Worker: {lock.owner}
                  </div>
                  {taskIdDisplay && (
                    <div className="text-xs text-zinc-500 mt-1 font-mono" title={taskId || undefined}>
                      Task: {taskIdDisplay !== taskId ? (
                        <>
                          <span aria-hidden="true">{taskIdDisplay}</span>
                          <span className="sr-only">{taskId}</span>
                        </>
                      ) : taskIdDisplay}
                    </div>
                  )}
                  <div className="text-xs text-zinc-500 mt-1">TTL: {formatTtl(lock.ttl)}</div>
                </Card>
              );
            })
          )}
        </Column>

        {/* Completed */}
        <Column header={columns[2]}>
          {completed.length === 0 ? (
            <EmptyState>{kanbanResultEmptyMessage("completed", resultCountsArePreview)}</EmptyState>
          ) : (
            <>
              {completed.slice(0, KANBAN_RESULT_COLUMN_LIMIT).map((r) => {
                const outputParams = taskOutputParamsForResult(r);
                const isSelected = taskOutputParamsEqual(selectedTask, outputParams);
                const resourceLabel = resultResourceLabel(r);
                const outputPanelId = outputParams ? taskOutputDomId(outputParams) : undefined;
                const cardDescriptionId = outputPanelId
                  ? `${outputPanelId}-card-description`
                  : undefined;
                return (
                  <Card
                    key={r.result_id}
                    className={`transition-colors ${
                      isSelected ? "border-emerald-500/50 ring-1 ring-emerald-500/30" :
                      outputParams ? "hover:border-zinc-600" : ""
                    } ${outputParams ? "cursor-pointer" : "cursor-default"}`}
                    onClick={outputParams
                      ? () => selectTask(outputParams, resourceLabel)
                      : undefined}
                    onFocus={() => rememberFocusedOutputTrigger(outputPanelId)}
                    triggerRef={outputParams && outputPanelId
                      ? trackOutputTrigger(outputPanelId)
                      : undefined}
                    ariaLabel={outputParams
                      ? taskOutputControlLabel(isSelected ? "Hide" : "View", resourceLabel, outputParams)
                      : undefined}
                    ariaControls={isSelected ? outputPanelId : undefined}
                    ariaDescribedBy={cardDescriptionId}
                    ariaExpanded={outputParams ? isSelected : undefined}
                    descriptionId={cardDescriptionId}
                  >
                    <div className="flex min-w-0 items-center justify-between gap-2 mb-2">
                      <span className="min-w-0 truncate font-mono text-sm" title={resourceLabel}>
                        {resourceLabel}
                      </span>
                      <span className="shrink-0 text-xs font-mono text-zinc-500">{formatDuration(r.duration_seconds)}</span>
                    </div>
                    <div className="text-xs text-zinc-500 truncate" title={r.result_stream}>
                      <span>{redisStreamDisplayName(r.result_stream)}</span>
                      <span className="sr-only"> raw stream {r.result_stream}</span>
                    </div>
                    <ResultMeta result={r} />
                    {r.summary && <div className="text-xs text-zinc-500 mt-1 line-clamp-2 break-words">{r.summary}</div>}
                    <div className={`text-xs mt-1 ${outputParams ? "text-emerald-400/60" : "text-zinc-600"}`}>
                      {outputParams ? (isSelected ? "Hide Output" : "View Output") : "No Output"}
                    </div>
                  </Card>
                );
              })}
              <ResultLimitNotice total={completed.length} loadedPreviewOnly={resultCountsArePreview} />
            </>
          )}
        </Column>

        {/* Needs Attention */}
        <Column header={columns[3]}>
          {failed.length === 0 ? (
            <EmptyState>{kanbanResultEmptyMessage("failed", resultCountsArePreview)}</EmptyState>
          ) : (
            <>
              {failed.slice(0, KANBAN_RESULT_COLUMN_LIMIT).map((r) => {
                const outputParams = taskOutputParamsForResult(r);
                const isSelected = taskOutputParamsEqual(selectedTask, outputParams);
                const resourceLabel = resultResourceLabel(r);
                const outputPanelId = outputParams ? taskOutputDomId(outputParams) : undefined;
                const cardDescriptionId = outputPanelId
                  ? `${outputPanelId}-card-description`
                  : undefined;
                return (
                  <Card
                    key={r.result_id}
                    className={`transition-colors border-red-500/20 ${
                      isSelected ? "ring-1 ring-red-500/30" :
                      outputParams ? "hover:border-zinc-600" : ""
                    } ${outputParams ? "cursor-pointer" : "cursor-default"}`}
                    onClick={outputParams
                      ? () => selectTask(outputParams, resourceLabel)
                      : undefined}
                    onFocus={() => rememberFocusedOutputTrigger(outputPanelId)}
                    triggerRef={outputParams && outputPanelId
                      ? trackOutputTrigger(outputPanelId)
                      : undefined}
                    ariaLabel={outputParams
                      ? taskOutputControlLabel(isSelected ? "Hide" : "View", resourceLabel, outputParams)
                      : undefined}
                    ariaControls={isSelected ? outputPanelId : undefined}
                    ariaDescribedBy={cardDescriptionId}
                    ariaExpanded={outputParams ? isSelected : undefined}
                    descriptionId={cardDescriptionId}
                  >
                    <div className="flex min-w-0 items-center justify-between gap-2 mb-2">
                      <span className="min-w-0 truncate font-mono text-sm" title={resourceLabel}>
                        {resourceLabel}
                      </span>
                      <span className={`shrink-0 text-xs rounded-full px-2 py-0.5 ${statusColor(r.status)}`}>
                        {resultStatusLabel(r.status)}
                      </span>
                    </div>
                    <div className="text-xs text-zinc-500 truncate" title={r.result_stream}>
                      <span>{redisStreamDisplayName(r.result_stream)}</span>
                      <span className="sr-only"> raw stream {r.result_stream}</span>
                    </div>
                    <ResultMeta result={r} includeDuration />
                    {r.summary && <div className="text-xs text-red-400/80 mt-1 line-clamp-2 break-words">{r.summary}</div>}
                    <div className={`text-xs mt-1 ${outputParams ? "text-red-400/60" : "text-zinc-600"}`}>
                      {outputParams ? (isSelected ? "Hide Output" : "View Output") : "No Output"}
                    </div>
                  </Card>
                );
              })}
              <ResultLimitNotice total={failed.length} loadedPreviewOnly={resultCountsArePreview} />
            </>
          )}
        </Column>

        {/* Skipped / Stale */}
        <Column header={columns[4]}>
          {neutral.length === 0 ? (
            <EmptyState>{kanbanResultEmptyMessage("neutral", resultCountsArePreview)}</EmptyState>
          ) : (
            <>
              {neutral.slice(0, KANBAN_RESULT_COLUMN_LIMIT).map((r) => {
                const outputParams = taskOutputParamsForResult(r);
                const isSelected = taskOutputParamsEqual(selectedTask, outputParams);
                const resourceLabel = resultResourceLabel(r);
                const outputPanelId = outputParams ? taskOutputDomId(outputParams) : undefined;
                const cardDescriptionId = outputPanelId
                  ? `${outputPanelId}-card-description`
                  : undefined;
                return (
                  <Card
                    key={r.result_id}
                    className={`transition-colors border-zinc-700/60 ${
                      isSelected ? "ring-1 ring-zinc-500/30" :
                      outputParams ? "hover:border-zinc-600" : ""
                    } ${outputParams ? "cursor-pointer" : "cursor-default"}`}
                    onClick={outputParams
                      ? () => selectTask(outputParams, resourceLabel)
                      : undefined}
                    onFocus={() => rememberFocusedOutputTrigger(outputPanelId)}
                    triggerRef={outputParams && outputPanelId
                      ? trackOutputTrigger(outputPanelId)
                      : undefined}
                    ariaLabel={outputParams
                      ? taskOutputControlLabel(isSelected ? "Hide" : "View", resourceLabel, outputParams)
                      : undefined}
                    ariaControls={isSelected ? outputPanelId : undefined}
                    ariaDescribedBy={cardDescriptionId}
                    ariaExpanded={outputParams ? isSelected : undefined}
                    descriptionId={cardDescriptionId}
                  >
                    <div className="flex min-w-0 items-center justify-between gap-2 mb-2">
                      <span className="min-w-0 truncate font-mono text-sm" title={resourceLabel}>
                        {resourceLabel}
                      </span>
                      <span className={`shrink-0 text-xs rounded-full px-2 py-0.5 ${statusColor(r.status)}`}>
                        {resultStatusLabel(r.status)}
                      </span>
                    </div>
                    <div className="text-xs text-zinc-500 truncate" title={r.result_stream}>
                      <span>{redisStreamDisplayName(r.result_stream)}</span>
                      <span className="sr-only"> raw stream {r.result_stream}</span>
                    </div>
                    <ResultMeta result={r} includeDuration />
                    {r.summary && <div className="text-xs text-zinc-500 mt-1 line-clamp-2 break-words">{r.summary}</div>}
                    <div className={`text-xs mt-1 ${outputParams ? "text-zinc-400/70" : "text-zinc-600"}`}>
                      {outputParams ? (isSelected ? "Hide Output" : "View Output") : "No Output"}
                    </div>
                  </Card>
                );
              })}
              <ResultLimitNotice total={neutral.length} loadedPreviewOnly={resultCountsArePreview} />
            </>
          )}
        </Column>
      </div>

      {/* Task Output Panel */}
      {selectedTask && (
        <TaskOutputPanel
          key={kanbanTaskOutputPanelKey(selectedTask)}
          id={taskOutputDomId(selectedTask)}
          params={selectedTask}
          label={selectedLabel}
          autoFocusLog={autoFocusSelectedTask}
          onClose={() => closeSelectedTask(true)}
          className={KANBAN_OUTPUT_PANEL_CLASS}
        />
      )}
    </div>
  );
}

function ResultMeta({
  result,
  includeDuration = false,
}: {
  result: RecentResult;
  includeDuration?: boolean;
}) {
  const timestamp = kanbanResultTimestampText(result);
  const duration = formatDuration(result.duration_seconds);
  return (
    <div className="mt-1 flex min-w-0 items-center justify-between gap-2 text-xs text-zinc-400">
      <span className="min-w-0 truncate" title={result.worker_id}>{result.worker_id}</span>
      <span
        className="shrink-0 whitespace-nowrap text-zinc-500"
        title={includeDuration
          ? `entry ${result.entry_id}; duration ${duration}`
          : `entry ${result.entry_id}`}
      >
        {includeDuration ? `${timestamp} - ${duration}` : timestamp}
      </span>
    </div>
  );
}

function ResultLimitNotice({
  total,
  loadedPreviewOnly,
}: {
  total: number;
  loadedPreviewOnly: boolean;
}) {
  const message = kanbanResultLimitMessage(total, loadedPreviewOnly);
  return message ? (
    <div className="rounded-md border border-zinc-800 bg-zinc-950 px-2 py-1.5 text-center text-xs text-zinc-500">
      {message}
    </div>
  ) : null;
}

function Column({
  header,
  children,
}: {
  header: { title: string; color: string; headerBg: string; count: string };
  children: React.ReactNode;
}) {
  const titleId = kanbanColumnTitleId(header.title);
  return (
    <div
      role="group"
      aria-labelledby={titleId}
      className={`flex min-h-0 flex-col min-w-[280px] w-[280px] rounded-lg border border-zinc-800 ${header.color} border-t-2`}
    >
      <div className={`px-3 py-2.5 ${header.headerBg} rounded-t-lg flex items-center justify-between`}>
        <span id={titleId} className="text-sm font-medium">{header.title}</span>
        <span className="text-xs text-zinc-400 bg-zinc-800 rounded-full px-2 py-0.5">{header.count}</span>
      </div>
      <div className="flex-1 overflow-y-auto p-2 space-y-2">
        {children}
      </div>
    </div>
  );
}

function Card({
  children,
  className = "",
  onClick,
  onFocus,
  triggerRef,
  ariaLabel,
  ariaControls,
  ariaDescribedBy,
  ariaExpanded,
  descriptionId,
}: {
  children: React.ReactNode;
  className?: string;
  onClick?: () => void;
  onFocus?: () => void;
  triggerRef?: RefCallback<HTMLButtonElement>;
  ariaLabel?: string;
  ariaControls?: string;
  ariaDescribedBy?: string;
  ariaExpanded?: boolean;
  descriptionId?: string;
}) {
  const cardClassName =
    `min-w-0 overflow-hidden rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2.5 text-left ${className}`;

  if (onClick) {
    return (
      <button
        type="button"
        ref={triggerRef}
        className={`${cardClassName} block w-full appearance-none focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-zinc-500`}
        onClick={onClick}
        onFocus={onFocus}
        aria-label={ariaLabel}
        aria-controls={ariaControls}
        aria-describedby={ariaDescribedBy}
        aria-expanded={ariaExpanded}
      >
        <div id={descriptionId}>
          {children}
        </div>
      </button>
    );
  }

  return (
    <div
      className={cardClassName}
    >
      {children}
    </div>
  );
}

function EmptyState({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-xs text-zinc-600 italic text-center py-4">{children}</div>
  );
}
