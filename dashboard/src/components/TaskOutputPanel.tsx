import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import { useTaskOutput } from "../hooks/useTaskOutput";
import type { TaskOutputParams } from "../lib/types";

interface Props {
  id: string;
  params: TaskOutputParams;
  label: string;
  onClose: () => void;
  className?: string;
  autoFocusLog?: boolean;
}

export type TaskOutputBodyMode = "empty" | "error-only" | "lines";
export type TaskOutputHeaderStatus = "live" | "reconnecting" | "unavailable" | "complete" | null;
export const TASK_OUTPUT_PANEL_DEFAULT_CLASS = "min-h-0 flex-1 border-t border-zinc-800 mt-3 pt-3";

export function taskOutputBodyMode(
  lineCount: number,
  error: string | null,
): TaskOutputBodyMode {
  if (lineCount > 0) return "lines";
  return error ? "error-only" : "empty";
}

export function taskOutputEmptyMessage(connected: boolean, done: boolean, retrying = false): string {
  if (done) return "No output captured";
  if (retrying) return "Reconnecting...";
  return connected ? "Waiting for output..." : "Connecting...";
}

export function taskOutputHeaderStatus(
  connected: boolean,
  done: boolean,
  error: string | null,
  lineCount: number,
  retrying = false,
): TaskOutputHeaderStatus {
  if (error) return "unavailable";
  if (done) return "complete";
  if (connected) return "live";
  if (retrying || lineCount > 0) return "reconnecting";
  return null;
}

export function taskOutputTaskIdDisplay(
  taskId: string | null | undefined,
  maxLength = 16,
): string | null {
  const trimmed = taskId?.trim();
  if (!trimmed) return null;
  if (trimmed.length <= maxLength) return trimmed;
  return `${trimmed.slice(0, maxLength)}...`;
}

export function taskOutputPrefixDisplay(prefix: string | null | undefined): string | null {
  if (prefix === undefined) return null;
  if (prefix === null) return "unprefixed";
  return prefix.trim() || "unprefixed";
}

export function taskOutputMetaTitle(
  workerId: string,
  taskId: string | null | undefined,
  prefix?: string | null,
): string {
  const taskIdValue = taskId?.trim();
  const details: string[] = [];
  const prefixDisplay = taskOutputPrefixDisplay(prefix);
  if (prefixDisplay) details.push(`prefix: ${prefixDisplay}`);
  details.push(`worker: ${workerId}`);
  if (taskIdValue) details.push(`task: ${taskIdValue}`);
  return details.join(" | ");
}

export function taskOutputPanelShouldCloseOnKey(key: string): boolean {
  return key === "Escape";
}

function statusClass(status: Exclude<TaskOutputHeaderStatus, null>): string {
  switch (status) {
    case "live":
      return "text-emerald-400";
    case "reconnecting":
      return "text-yellow-400";
    case "unavailable":
      return "text-yellow-400";
    case "complete":
      return "text-zinc-500";
  }
}

function statusLabel(status: Exclude<TaskOutputHeaderStatus, null>): string {
  switch (status) {
    case "live":
      return "Live";
    case "reconnecting":
      return "Reconnecting";
    case "unavailable":
      return "Unavailable";
    case "complete":
      return "Complete";
  }
}

export function TaskOutputPanel({
  id,
  params,
  label,
  onClose,
  className = TASK_OUTPUT_PANEL_DEFAULT_CLASS,
  autoFocusLog = true,
}: Props) {
  const { lines, startIndex, connected, retrying, done, error } = useTaskOutput(params);
  const logRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);
  const bodyMode = taskOutputBodyMode(lines.length, error);
  const headerStatus = taskOutputHeaderStatus(connected, done, error, lines.length, retrying);
  const taskIdDisplay = taskOutputTaskIdDisplay(params.taskId);
  const prefixDisplay = taskOutputPrefixDisplay(params.prefix);
  const fullTaskId = typeof params.taskId === "string" ? params.taskId.trim() : "";
  const showFullTaskId = fullTaskId !== "" && taskIdDisplay !== fullTaskId;
  const titleId = `${id}-title`;

  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (!taskOutputPanelShouldCloseOnKey(event.key)) return;
    event.preventDefault();
    event.stopPropagation();
    onClose();
  };

  useEffect(() => {
    if (!autoFocusLog) return;
    logRef.current?.focus();
  }, [autoFocusLog]);

  useEffect(() => {
    if (autoScroll && logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [lines, autoScroll]);

  const handleScroll = () => {
    if (!logRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = logRef.current;
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 50);
  };

  return (
    <div
      id={id}
      role="region"
      aria-labelledby={titleId}
      onKeyDown={handleKeyDown}
      className={`flex flex-col ${className}`}
    >
      <div className="mb-2 flex flex-wrap items-center justify-between gap-2 px-1">
        <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
          <span
            id={titleId}
            className="min-w-0 truncate text-sm font-medium text-zinc-300"
            title={label}
          >
            Output: {label}
          </span>
          <span
            className="min-w-0 truncate text-xs text-zinc-500"
            title={taskOutputMetaTitle(params.workerId, params.taskId, params.prefix)}
          >
            {prefixDisplay && `prefix: ${prefixDisplay} | `}
            worker: {params.workerId}
            {taskIdDisplay && ` | task: ${taskIdDisplay}`}
            {showFullTaskId && <span className="sr-only"> full task: {fullTaskId}</span>}
          </span>
          {headerStatus && (
            <span
              role="status"
              aria-live="polite"
              aria-atomic="true"
              className={`inline-flex shrink-0 items-center gap-1 text-xs ${statusClass(headerStatus)}`}
            >
              {(headerStatus === "live" || headerStatus === "reconnecting") && (
                <span className={`h-1.5 w-1.5 rounded-full animate-pulse ${
                  headerStatus === "live" ? "bg-emerald-400" : "bg-yellow-400"
                }`} />
              )}
              {statusLabel(headerStatus)}
            </span>
          )}
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {!autoScroll && (
            <button
              type="button"
              aria-label={`Scroll output for ${label} to bottom`}
              className="rounded border border-zinc-700 px-2 py-0.5 text-xs text-zinc-400 hover:text-zinc-200"
              onClick={() => {
                setAutoScroll(true);
                if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
              }}
            >
              Scroll to bottom
            </button>
          )}
          <button
            type="button"
            aria-label={`Close output for ${label}`}
            className="rounded border border-zinc-700 px-2 py-0.5 text-xs text-zinc-400 hover:text-zinc-200"
            onClick={onClose}
          >
            Close
          </button>
        </div>
      </div>
      <div
        ref={logRef}
        role="log"
        aria-label={`Output log for ${label}`}
        aria-live={done && !error ? "off" : "polite"}
        aria-relevant="additions text"
        aria-atomic="false"
        tabIndex={0}
        onScroll={handleScroll}
        className="min-h-0 flex-1 overflow-auto rounded-lg border border-zinc-800 bg-zinc-950 p-4 font-mono text-xs leading-relaxed text-zinc-300 focus:outline-none focus:ring-2 focus:ring-sky-500/60"
      >
        {error && bodyMode !== "error-only" && (
          <div
            role="alert"
            className="mb-3 rounded border border-yellow-900/60 bg-yellow-950/30 px-3 py-2 text-yellow-300"
          >
            {error}
          </div>
        )}
        {bodyMode === "error-only" ? (
          <div role="alert" className="text-yellow-300">{error}</div>
        ) : bodyMode === "empty" ? (
          <div className="text-zinc-600 italic">
            {taskOutputEmptyMessage(connected, done, retrying)}
          </div>
        ) : (
          lines.map((line, i) => (
            <div
              key={startIndex + i}
              className={
                line.startsWith("---") || line.startsWith("───")
                  ? "mt-2 border-t border-zinc-800 pt-2 font-bold text-cyan-400"
                  : line.startsWith("  $")
                    ? "text-yellow-300"
                    : line.startsWith("  ")
                      ? "text-zinc-500"
                      : ""
              }
            >
              <pre className="whitespace-pre-wrap break-words">{line}</pre>
            </div>
          ))
        )}
      </div>
    </div>
  );
}
