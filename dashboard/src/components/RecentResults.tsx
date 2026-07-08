import { useEffect, useMemo, useRef, useState, type RefCallback } from "react";
import type { RecentResult, TaskOutputParams } from "../lib/types";
import { statusColor, formatDuration, formatTimestampMs } from "../lib/format";
import { degradedCountLabel, loadedCountLabel } from "../lib/counts";
import { deferFocus } from "../lib/focus";
import { redisStreamDisplayName } from "../lib/queues";
import {
  partitionResultsByStatus,
  type ResultColumn,
  resultResourceLabel,
  resultStatusLabel,
  resultTimestampMs,
  resultTypeLabel,
} from "../lib/results";
import {
  taskOutputControlLabel,
  taskOutputDomId,
  taskOutputParamsEqual,
  taskOutputParamsForResult,
  taskOutputParamsInstanceEqual,
  taskOutputParamsTargetEqual,
} from "../lib/taskOutput";
import { TaskOutputPanel } from "./TaskOutputPanel";

export type RecentResultFilter = "all" | ResultColumn;

interface Props {
  results: RecentResult[];
  total?: number;
  degraded?: boolean;
  depthDegraded?: boolean;
  filter?: RecentResultFilter;
  onFilterChange?: (filter: RecentResultFilter) => void;
}

export function resultIdStillVisible(resultId: string | null, results: RecentResult[]): boolean {
  return resultId !== null && results.some((result) => result.result_id === resultId);
}

export function recentResultsEmptyMessage(
  degraded: boolean,
  depthDegraded = false,
  total = 0,
): string {
  if (degraded) return "Recent results unavailable";
  return depthDegraded || total > 0 ? "No recent results loaded" : "No results yet";
}

export function recentSummaryToggleLabel(resourceLabel: string, expanded: boolean): string {
  return `${expanded ? "Collapse" : "Show full"} summary for ${resourceLabel}`;
}

export function recentResultTaskOutputPanelKey(params: TaskOutputParams): string {
  return taskOutputDomId(params, "results-output-panel");
}

export function recentResultFilterLabel(filter: RecentResultFilter): string {
  switch (filter) {
    case "all":
      return "All";
    case "failed":
      return "Needs attention";
    case "completed":
      return "Completed";
    case "neutral":
      return "Other";
  }
}

export function filterRecentResults(
  results: RecentResult[],
  filter: RecentResultFilter,
): RecentResult[] {
  if (filter === "all") return results;
  return partitionResultsByStatus(results)[filter];
}

export function recentResultFilterEmptyMessage(
  filter: RecentResultFilter,
  degraded: boolean,
  depthDegraded = false,
  total = 0,
): string {
  if (filter === "all") return recentResultsEmptyMessage(degraded, depthDegraded, total);
  if (degraded) {
    switch (filter) {
      case "failed":
        return "No loaded results need attention";
      case "completed":
        return "No completions in loaded results";
      case "neutral":
        return "No other results in loaded results";
    }
  }
  switch (filter) {
    case "failed":
      return "No loaded results need attention";
    case "completed":
      return "No loaded completions";
    case "neutral":
      return "No loaded other results";
  }
}

export function recentResultFilterCounts(results: RecentResult[]): Record<RecentResultFilter, number> {
  const { completed, failed, neutral } = partitionResultsByStatus(results);
  return {
    all: results.length,
    completed: completed.length,
    failed: failed.length,
    neutral: neutral.length,
  };
}

export function recentResultFilterCountLabel(
  count: number,
  loadedPreviewOnly = false,
  degraded = false,
): string {
  return loadedPreviewOnly ? loadedCountLabel(count, degraded) : degradedCountLabel(count, degraded);
}

export function recentResultsHeadingText(
  loaded: number,
  total = loaded,
  entriesDegraded = false,
  depthDegraded = false,
): string {
  if (depthDegraded) {
    return loaded > 0
      ? `Recent Results (${loaded} loaded, ? total)`
      : "Recent Results (? total)";
  }

  const normalizedTotal = Math.max(loaded, total);
  if ((entriesDegraded && loaded > 0) || normalizedTotal > loaded) {
    return `Recent Results (${loaded} loaded, ${normalizedTotal} total)`;
  }

  return `Recent Results (${degradedCountLabel(loaded, entriesDegraded)})`;
}

export function RecentResults({
  results,
  total = results.length,
  degraded = false,
  depthDegraded = false,
  filter,
  onFilterChange,
}: Props) {
  const [internalFilter, setInternalFilter] = useState<RecentResultFilter>("all");
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [selectedOutput, setSelectedOutput] = useState<TaskOutputParams | null>(null);
  const [selectedOutputLabel, setSelectedOutputLabel] = useState("");
  const [autoFocusSelectedOutput, setAutoFocusSelectedOutput] = useState(false);
  const headingRef = useRef<HTMLHeadingElement>(null);
  const outputTriggerRefs = useRef(new Map<string, HTMLButtonElement>());
  const focusedOutputTriggerIdRef = useRef<string | null>(null);
  const activeFilter = filter ?? internalFilter;
  const normalizedTotal = Math.max(results.length, total);
  const filterCountsArePreview =
    degraded || depthDegraded || normalizedTotal > results.length;
  const visibleResults = useMemo(
    () => filterRecentResults(results, activeFilter),
    [results, activeFilter],
  );
  const filterCounts = useMemo(() => recentResultFilterCounts(results), [results]);

  const setFilter = (nextFilter: RecentResultFilter) => {
    if (filter === undefined) setInternalFilter(nextFilter);
    onFilterChange?.(nextFilter);
  };

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

  const closeSelectedOutput = (restoreFocus = false) => {
    const output = selectedOutput;
    setSelectedOutput(null);
    setSelectedOutputLabel("");
    setAutoFocusSelectedOutput(false);
    if (restoreFocus && output) {
      const triggerId = taskOutputDomId(output, "results-output");
      deferFocus(() => outputTriggerRefs.current.get(triggerId) ?? headingRef.current);
    }
  };

  const selectedOutputContainsFocus = () => {
    if (!selectedOutput || typeof document === "undefined") return false;
    const triggerId = taskOutputDomId(selectedOutput, "results-output");
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

  useEffect(() => {
    if (!expandedId) return;
    if (!resultIdStillVisible(expandedId, visibleResults)) {
      setExpandedId(null);
    }
  }, [expandedId, visibleResults]);

  useEffect(() => {
    if (!selectedOutput) return;
    const visibleCandidates = visibleResults.map((result) => ({
      params: taskOutputParamsForResult(result),
      label: resultResourceLabel(result),
    }));
    const exactMatch = visibleCandidates.find((candidate) =>
      taskOutputParamsEqual(selectedOutput, candidate.params)
    );
    const instanceMatch = exactMatch
      ? undefined
      : visibleCandidates.find((candidate) =>
        taskOutputParamsInstanceEqual(selectedOutput, candidate.params)
      );
    const targetMatches = exactMatch || instanceMatch
      ? []
      : visibleCandidates.filter((candidate) =>
        taskOutputParamsTargetEqual(selectedOutput, candidate.params)
      );
    const match =
      exactMatch ||
      instanceMatch ||
      (targetMatches.length === 1 ? targetMatches[0] : undefined);
    if (match) {
      if (!taskOutputParamsEqual(selectedOutput, match.params)) {
        setAutoFocusSelectedOutput(selectedOutputContainsFocus());
        setSelectedOutput(match.params);
      }
      setSelectedOutputLabel((current) => current === match.label ? current : match.label);
      return;
    }

    const loadedCandidates = results.map((result) => taskOutputParamsForResult(result));
    const stillLoaded = loadedCandidates.some((candidate) =>
      taskOutputParamsEqual(selectedOutput, candidate) ||
      taskOutputParamsInstanceEqual(selectedOutput, candidate)
    );
    if (stillLoaded) {
      closeSelectedOutput(selectedOutputContainsFocus());
      return;
    }
    if (degraded) return;
    closeSelectedOutput(selectedOutputContainsFocus());
  }, [degraded, results, selectedOutput, visibleResults]);

  const toggleExpanded = (resultId: string) => {
    setExpandedId(expandedId === resultId ? null : resultId);
  };
  const selectOutput = (params: TaskOutputParams | null, label: string) => {
    if (!params) return;
    if (taskOutputParamsEqual(selectedOutput, params)) {
      closeSelectedOutput();
      return;
    }
    setAutoFocusSelectedOutput(true);
    setSelectedOutput(params);
    setSelectedOutputLabel(label);
  };

  const summaryText = (result: RecentResult, expanded: boolean) =>
    expanded || result.summary.length <= 80
      ? result.summary
      : result.summary.slice(0, 80) + "...";

  return (
    <div>
      <h2
        ref={headingRef}
        tabIndex={-1}
        className="mb-3 rounded-sm text-sm font-medium text-zinc-400 focus:outline-none focus:ring-2 focus:ring-zinc-500"
      >
        {recentResultsHeadingText(results.length, total, degraded, depthDegraded)}
      </h2>
      {results.length === 0 ? (
        <div className="text-sm text-zinc-500 italic">
          {recentResultsEmptyMessage(degraded, depthDegraded, normalizedTotal)}
        </div>
      ) : (
        <>
          <div
            className="mb-3 flex flex-wrap gap-1"
            role="group"
            aria-label="Recent result status filter"
          >
            {(["all", "failed", "completed", "neutral"] as const).map((filterOption) => {
              const selected = activeFilter === filterOption;
              const count = recentResultFilterCountLabel(
                filterCounts[filterOption],
                filterCountsArePreview,
                degraded,
              );
              return (
                <button
                  key={filterOption}
                  type="button"
                  aria-pressed={selected}
                  className={`rounded-md border px-2.5 py-1 text-xs transition-colors ${
                    selected
                      ? "border-zinc-500 bg-zinc-800 text-zinc-100"
                      : "border-zinc-800 text-zinc-400 hover:border-zinc-700 hover:text-zinc-200"
                  }`}
                  onClick={() => setFilter(filterOption)}
                >
                  {recentResultFilterLabel(filterOption)} {count}
                </button>
              );
            })}
          </div>
          {visibleResults.length === 0 ? (
            <div className="text-sm text-zinc-500 italic">
              {recentResultFilterEmptyMessage(activeFilter, degraded, depthDegraded, normalizedTotal)}
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-sm" aria-label="Recent results">
                <thead>
                  <tr className="border-b border-zinc-800 text-left text-zinc-500">
                    <th scope="col" className="pb-2 pr-4">Status</th>
                    <th scope="col" className="pb-2 pr-4">Time</th>
                    <th scope="col" className="pb-2 pr-4">Stream</th>
                    <th scope="col" className="pb-2 pr-4">Type</th>
                    <th scope="col" className="pb-2 pr-4">Resource</th>
                    <th scope="col" className="pb-2 pr-4">Worker</th>
                    <th scope="col" className="pb-2 pr-4">Duration</th>
                    <th scope="col" className="pb-2 pr-4">Output</th>
                    <th scope="col" className="pb-2">Summary</th>
                  </tr>
                </thead>
                <tbody>
                  {visibleResults.map((r) => {
                    const expanded = expandedId === r.result_id;
                    const canExpandSummary = r.summary.length > 80;
                    const summary = summaryText(r, expanded) || "(no summary)";
                    const resourceLabel = resultResourceLabel(r);
                    const statusLabel = resultStatusLabel(r.status);
                    const outputParams = taskOutputParamsForResult(r);
                    const outputSelected = taskOutputParamsEqual(selectedOutput, outputParams);
                    const outputPanelId = outputParams ? taskOutputDomId(outputParams, "results-output") : undefined;
                    const streamDisplay = redisStreamDisplayName(r.result_stream);
                    const timestamp = formatTimestampMs(resultTimestampMs(r.entry_id));
                    return (
                      <tr
                        key={r.result_id}
                        className={`border-b border-zinc-800/50 hover:bg-zinc-800/20 ${
                          outputSelected ? "bg-zinc-800/30" : ""
                        }`}
                      >
                        <td className="py-2 pr-4">
                          <span
                            className={`inline-flex items-center rounded-full px-2 py-0.5 text-xs ${statusColor(r.status)}`}
                          >
                            {statusLabel}
                          </span>
                        </td>
                        <td
                          className="whitespace-nowrap py-2 pr-4 text-zinc-400"
                          title={`entry ${r.entry_id}`}
                        >
                          {timestamp}
                        </td>
                        <td
                          className="max-w-[18rem] truncate py-2 pr-4 font-mono text-xs text-zinc-500"
                          title={r.result_stream}
                        >
                          <span>{streamDisplay}</span>
                          <span className="sr-only"> raw stream {r.result_stream}</span>
                        </td>
                        <td className="py-2 pr-4 text-zinc-400">{resultTypeLabel(r.resource_type)}</td>
                        <th
                          scope="row"
                          className="max-w-[18rem] truncate py-2 pr-4 text-left font-mono font-normal"
                          title={resourceLabel}
                        >
                          {resourceLabel}
                        </th>
                        <td
                          className="max-w-[16rem] truncate py-2 pr-4 text-zinc-400"
                          title={r.worker_id}
                        >
                          {r.worker_id}
                        </td>
                        <td className="py-2 pr-4 font-mono">
                          {formatDuration(r.duration_seconds)}
                        </td>
                        <td className="py-2 pr-4">
                          {outputParams ? (
                            <button
                              type="button"
                              ref={outputPanelId ? trackOutputTrigger(outputPanelId) : undefined}
                              className={`rounded border px-2 py-1 text-xs ${
                                outputSelected
                                  ? "border-sky-500/50 bg-sky-500/10 text-sky-300"
                                  : "border-zinc-700 text-zinc-400 hover:text-zinc-200"
                              }`}
                              onFocus={() => {
                                if (outputPanelId) focusedOutputTriggerIdRef.current = outputPanelId;
                              }}
                              onClick={() => selectOutput(outputParams, resourceLabel)}
                              aria-controls={outputSelected ? outputPanelId : undefined}
                              aria-expanded={outputSelected}
                              aria-label={taskOutputControlLabel(
                                outputSelected ? "Hide" : "View",
                                resourceLabel,
                                outputParams,
                              )}
                            >
                              {outputSelected ? "Hide" : "View"}
                            </button>
                          ) : (
                            <span className="text-xs text-zinc-600">none</span>
                          )}
                        </td>
                        <td className="max-w-[32rem] py-2 text-zinc-300">
                          {canExpandSummary ? (
                            <button
                              type="button"
                              className="block w-full rounded text-left break-words hover:text-zinc-100 focus:outline-none focus:ring-1 focus:ring-zinc-500"
                              onClick={() => toggleExpanded(r.result_id)}
                              aria-expanded={expanded}
                              aria-label={recentSummaryToggleLabel(resourceLabel, expanded)}
                              title={r.summary || undefined}
                            >
                              {summary}
                            </button>
                          ) : (
                            <span className="break-words" title={r.summary || undefined}>{summary}</span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
      {selectedOutput && (
        <TaskOutputPanel
          key={recentResultTaskOutputPanelKey(selectedOutput)}
          id={taskOutputDomId(selectedOutput, "results-output")}
          params={selectedOutput}
          label={selectedOutputLabel}
          className="mt-4 h-[24rem]"
          autoFocusLog={autoFocusSelectedOutput}
          onClose={() => closeSelectedOutput(true)}
        />
      )}
    </div>
  );
}
