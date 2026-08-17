import { useEffect, useRef, useState, type RefCallback } from "react";
import type { LockInfo, StuckTask, TaskOutputParams } from "../lib/types";
import { formatTtl } from "../lib/format";
import { degradedCountLabel } from "../lib/counts";
import { deferFocus } from "../lib/focus";
import { lockMatchesStuck, resourceLabel, stuckTaskKeys } from "../lib/resource";
import {
  taskOutputControlLabel,
  taskOutputDomId,
  taskOutputParamsEqual,
  taskOutputParamsForLock,
  taskOutputParamsInstanceEqual,
  taskOutputParamsTargetEqual,
  taskOutputSelectionStillVisible,
} from "../lib/taskOutput";
import { TaskOutputPanel } from "./TaskOutputPanel";

interface Props {
  locks: LockInfo[];
  stuckTasks: StuckTask[];
  degraded?: boolean;
}

export function activeWorkEmptyMessage(degraded: boolean): string {
  return degraded ? "Active locks unavailable" : "No active locks";
}

export function activeWorkTaskOutputPanelKey(params: TaskOutputParams): string {
  return taskOutputDomId(params, "active-output-panel");
}

export function activeWorkTaskIdDisplay(
  taskId: string | null | undefined,
  maxLength = 16,
): string {
  const trimmed = taskId?.trim();
  if (!trimmed) return "?";
  return trimmed.length <= maxLength ? trimmed : `${trimmed.slice(0, maxLength)}...`;
}

export function ActiveWork({ locks, stuckTasks, degraded = false }: Props) {
  const stuckKeys = stuckTaskKeys(stuckTasks);
  const [selectedOutput, setSelectedOutput] = useState<TaskOutputParams | null>(null);
  const [selectedOutputLabel, setSelectedOutputLabel] = useState("");
  const [autoFocusSelectedOutput, setAutoFocusSelectedOutput] = useState(false);
  const headingRef = useRef<HTMLHeadingElement>(null);
  const outputTriggerRefs = useRef(new Map<string, HTMLButtonElement>());
  const focusedOutputTriggerIdRef = useRef<string | null>(null);

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
      const triggerId = taskOutputDomId(output, "active-output");
      deferFocus(() => outputTriggerRefs.current.get(triggerId) ?? headingRef.current);
    }
  };

  const selectedOutputContainsFocus = () => {
    if (!selectedOutput || typeof document === "undefined") return false;
    const triggerId = taskOutputDomId(selectedOutput, "active-output");
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
    if (!selectedOutput) return;
    const candidates = locks.map((lock) => ({
      params: taskOutputParamsForLock(lock),
      label: resourceLabel(lock),
    }));
    const exactMatch = candidates.find((candidate) =>
      taskOutputParamsEqual(selectedOutput, candidate.params)
    );
    const instanceMatch = exactMatch
      ? undefined
      : candidates.find((candidate) =>
        taskOutputParamsInstanceEqual(selectedOutput, candidate.params)
      );
    const targetMatches = exactMatch || instanceMatch
      ? []
      : candidates.filter((candidate) =>
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
    if (!taskOutputSelectionStillVisible(
      selectedOutput,
      candidates.map((candidate) => candidate.params),
      degraded,
    )) {
      closeSelectedOutput(selectedOutputContainsFocus());
    }
  }, [degraded, locks, selectedOutput]);

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

  return (
    <div>
      <h2
        ref={headingRef}
        tabIndex={-1}
        className="mb-3 rounded-sm text-sm font-medium text-zinc-400 focus:outline-none focus:ring-2 focus:ring-zinc-500"
      >
        Active Work ({degradedCountLabel(locks.length, degraded)})
      </h2>
      {locks.length === 0 ? (
        <div className="text-sm text-zinc-500 italic">
          {activeWorkEmptyMessage(degraded)}
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm" aria-label="Active work">
            <thead>
              <tr className="border-b border-zinc-800 text-left text-zinc-500">
                <th scope="col" className="pb-2 pr-4">Resource</th>
                <th scope="col" className="pb-2 pr-4">Worker</th>
                <th scope="col" className="pb-2 pr-4">Task</th>
                <th scope="col" className="pb-2 pr-4">TTL</th>
                <th scope="col" className="pb-2">Status / Output</th>
              </tr>
            </thead>
            <tbody>
              {locks.map((lock) => {
                const isStuck = lockMatchesStuck(lock, stuckKeys);
                const label = resourceLabel(lock, { includeRepo: false });
                const fullLabel = resourceLabel(lock);
                const outputParams = taskOutputParamsForLock(lock);
                const outputSelected = taskOutputParamsEqual(selectedOutput, outputParams);
                const outputPanelId = outputParams ? taskOutputDomId(outputParams, "active-output") : undefined;
                const taskId = lock.task_id?.trim() || "";
                const taskIdDisplay = activeWorkTaskIdDisplay(taskId);
                const showFullTaskId = taskId !== "" && taskIdDisplay !== taskId;
                return (
                  <tr
                    key={lock.lock_key}
                    className={`border-b border-zinc-800/50 hover:bg-zinc-800/20 ${
                      outputSelected ? "bg-zinc-800/30" : ""
                    }`}
                  >
                    <th scope="row" className="max-w-[16rem] py-2 pr-4 text-left font-mono font-normal">
                      <div className="truncate" title={fullLabel}>
                        {label}
                      </div>
                      <div className="truncate text-xs text-zinc-600" title={lock.repo}>
                        {lock.repo}
                      </div>
                    </th>
                    <td className="max-w-[18rem] truncate py-2 pr-4 text-zinc-400" title={lock.owner}>
                      {lock.owner}
                    </td>
                    <td
                      className="max-w-[14rem] truncate py-2 pr-4 font-mono text-xs text-zinc-400"
                      title={taskId || undefined}
                    >
                      {showFullTaskId ? (
                        <>
                          <span aria-hidden="true">{taskIdDisplay}</span>
                          <span className="sr-only">{taskId}</span>
                        </>
                      ) : taskIdDisplay}
                    </td>
                    <td className="py-2 pr-4 font-mono">{formatTtl(lock.ttl)}</td>
                    <td className="py-2 pr-4">
                      <div className="flex flex-wrap items-center gap-2">
                        {isStuck ? (
                          <span className="inline-flex items-center rounded-full bg-red-500/20 px-2 py-0.5 text-xs text-red-400">
                            Stuck
                          </span>
                        ) : (
                          <span className="inline-flex items-center rounded-full bg-emerald-500/20 px-2 py-0.5 text-xs text-emerald-400">
                            Running
                          </span>
                        )}
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
                            onClick={() => selectOutput(outputParams, fullLabel)}
                            aria-controls={outputSelected ? outputPanelId : undefined}
                            aria-expanded={outputSelected}
                            aria-label={taskOutputControlLabel(
                              outputSelected ? "Hide" : "View",
                              fullLabel,
                              outputParams,
                            )}
                          >
                            {outputSelected ? "Hide output" : "View output"}
                          </button>
                        ) : (
                          <span className="text-xs text-zinc-600">No output</span>
                        )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {selectedOutput && (
        <TaskOutputPanel
          key={activeWorkTaskOutputPanelKey(selectedOutput)}
          id={taskOutputDomId(selectedOutput, "active-output")}
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
