import type { LockInfo, RecentResult, TaskOutputMessage, TaskOutputParams } from "./types";

const REDIS_STREAM_ID_RE = /^\d+-\d+$/;
export const TASK_OUTPUT_MAX_LINES = 5000;
export const TASK_OUTPUT_PROTOCOL_ERROR =
  "Task output stream sent output without a resumable cursor.";
export const TASK_OUTPUT_MALFORMED_MESSAGE_ERROR =
  "Task output stream sent a malformed message.";

export interface TaskOutputClientState {
  lines: string[];
  startIndex: number;
  connected: boolean;
  retrying: boolean;
  done: boolean;
  error: string | null;
}

function usableWorkerId(workerId: string): string | null {
  const trimmed = workerId.trim();
  if (!trimmed || trimmed === "(expired)") return null;
  return trimmed;
}

function usableTaskId(taskId: string | null | undefined): string | null {
  const trimmed = (taskId || "").trim();
  return trimmed || null;
}

export function taskOutputParamsForLock(lock: LockInfo): TaskOutputParams | null {
  const workerId = usableWorkerId(lock.owner);
  const taskId = usableTaskId(lock.task_id);
  if (!workerId || !taskId) return null;

  return {
    workerId,
    taskId,
    historical: false,
    prefix: taskOutputPrefix(
      lock.output_prefix,
      lock.prefix ?? null,
      lock.output_prefix_unresolved,
    ),
    instanceId: lock.lock_key,
  };
}

function resultStreamPrefix(resultStream: string): string | null | undefined {
  const trimmed = resultStream.trim();
  if (trimmed === "results") return null;
  const prefixed = trimmed.match(/^(.+):results$/);
  return prefixed ? prefixed[1] : undefined;
}

export function taskOutputParamsForResult(result: RecentResult): TaskOutputParams | null {
  const workerId = usableWorkerId(result.worker_id);
  const taskId = usableTaskId(result.task_id);
  if (!workerId || !taskId) return null;

  const prefix = taskOutputPrefix(
    result.output_prefix,
    resultStreamPrefix(result.result_stream),
    result.output_prefix_unresolved,
  );
  return {
    workerId,
    taskId,
    historical: true,
    instanceId: result.result_id,
    ...(prefix !== undefined ? { prefix } : {}),
  };
}

function taskOutputPrefix(
  outputPrefix: string | null | undefined,
  fallbackPrefix: string | null | undefined,
  outputPrefixUnresolved = false,
): string | null | undefined {
  if (outputPrefix === undefined && outputPrefixUnresolved) {
    return undefined;
  }
  if (outputPrefix !== undefined) {
    return typeof outputPrefix === "string"
      ? (outputPrefix.trim() || null)
      : null;
  }
  return fallbackPrefix;
}

function taskOutputInstanceId(params: TaskOutputParams): string | undefined {
  return params.instanceId?.trim() || undefined;
}

export function taskOutputParamsEqual(
  a: TaskOutputParams | null,
  b: TaskOutputParams | null,
): boolean {
  if (!a || !b) return false;
  const aInstanceId = taskOutputInstanceId(a);
  const bInstanceId = taskOutputInstanceId(b);
  return (
    aInstanceId === bInstanceId &&
    taskOutputParamsTargetEqual(a, b)
  );
}

function taskOutputParamsBaseTargetEqual(
  a: TaskOutputParams | null,
  b: TaskOutputParams | null,
): boolean {
  if (!a || !b) return false;
  return (
    a.workerId === b.workerId &&
    a.taskId === b.taskId &&
    Boolean(a.historical) === Boolean(b.historical)
  );
}

export function taskOutputParamsTargetEqual(
  a: TaskOutputParams | null,
  b: TaskOutputParams | null,
): boolean {
  if (!a || !b || !taskOutputParamsBaseTargetEqual(a, b)) return false;
  return a.prefix === b.prefix;
}

export function taskOutputParamsInstanceEqual(
  a: TaskOutputParams | null,
  b: TaskOutputParams | null,
): boolean {
  const aInstanceId = a ? taskOutputInstanceId(a) : undefined;
  const bInstanceId = b ? taskOutputInstanceId(b) : undefined;
  return Boolean(
    aInstanceId &&
    bInstanceId &&
    aInstanceId === bInstanceId &&
    taskOutputParamsBaseTargetEqual(a, b),
  );
}

export function taskOutputSelectionStillVisible(
  selected: TaskOutputParams | null,
  candidates: Array<TaskOutputParams | null>,
  degraded = false,
): boolean {
  if (!selected) return false;
  if (degraded) return true;
  return candidates.some((candidate) =>
    taskOutputParamsEqual(selected, candidate) ||
    taskOutputParamsInstanceEqual(selected, candidate)
  );
}

function encodeDomIdPart(value: string): string {
  const encoded = Array.from(value, (char) => {
    const codePoint = char.codePointAt(0);
    return codePoint === undefined ? "" : codePoint.toString(16);
  }).filter(Boolean).join("_");
  return encoded || "0";
}

function encodePrefixDomIdPart(prefix: string | null | undefined): string {
  if (prefix === undefined) return "u";
  if (prefix === null) return "n";
  return `v${encodeDomIdPart(prefix)}`;
}

export function taskOutputDomId(params: TaskOutputParams, prefix = "task-output"): string {
  const parts = [
    prefix,
    `p${encodePrefixDomIdPart(params.prefix)}`,
    `w${encodeDomIdPart(params.workerId)}`,
    `t${encodeDomIdPart(params.taskId || "")}`,
    `h${params.historical ? "1" : "0"}`,
  ];
  const instanceId = taskOutputInstanceId(params);
  if (instanceId) parts.push(`i${encodeDomIdPart(instanceId)}`);
  return parts.join("-");
}

export function taskOutputControlLabel(
  action: "View" | "Hide",
  label: string,
  params: TaskOutputParams,
): string {
  const details = [params.workerId];
  if (params.prefix) details.push(`prefix ${params.prefix}`);
  const taskId = params.taskId?.trim();
  if (taskId) details.push(`task ${taskId}`);
  if (params.historical) details.push("historical");
  return `${action} output for ${label} (${details.join(", ")})`;
}

export function taskOutputCloseMessage(code: number, reason: string): string | null {
  const detail = reason.trim();
  if (code === 1008) {
    return detail || "Task output request was rejected.";
  }
  if (code === 1013) {
    return detail || "Too many task output streams are open.";
  }
  return null;
}

export function taskOutputCloseStatus(
  code: number,
  reason: string,
): { terminal: boolean; error: string | null } {
  const error = taskOutputCloseMessage(code, reason);
  if (error) return { terminal: true, error };

  const detail = reason.trim();
  if (code === 1000 && detail === "Task output complete") {
    return { terminal: true, error: null };
  }
  if (code === 1000 && detail === "Task output unavailable") {
    return { terminal: true, error: "Task output is unavailable." };
  }

  return { terminal: false, error: null };
}

export function taskOutputTerminalError(
  existingError: string | null,
  closeError: string | null,
): string | null {
  return existingError || closeError;
}

export function taskOutputMessageMarksDone(msg: TaskOutputMessage): boolean {
  return Boolean(msg.error?.trim()) || msg.done;
}

export function applyTaskOutputMessage(
  prev: TaskOutputClientState,
  msg: TaskOutputMessage,
  maxLines = TASK_OUTPUT_MAX_LINES,
): TaskOutputClientState {
  const error = msg.error?.trim();
  if (error) {
    return {
      ...prev,
      retrying: false,
      done: true,
      error,
    };
  }

  if (msg.lines.length > 0) {
    const newLines = [...prev.lines, ...msg.lines];
    if (newLines.length > maxLines) {
      const sliced = newLines.length - maxLines;
      return {
        ...prev,
        lines: newLines.slice(sliced),
        startIndex: prev.startIndex + sliced,
        retrying: false,
        done: msg.done,
        error: null,
      };
    }
    return {
      ...prev,
      lines: newLines,
      retrying: false,
      done: msg.done,
      error: null,
    };
  }

  if (msg.done) {
    return { ...prev, retrying: false, done: true, error: null };
  }

  return prev;
}

export function normalizeTaskOutputCursor(cursor: string | null | undefined): string | null {
  const trimmed = (cursor || "").trim();
  if (!trimmed || trimmed === "0-0") return null;
  return REDIS_STREAM_ID_RE.test(trimmed) ? trimmed : null;
}

function taskOutputCursorParts(cursor: string | null | undefined): [number, number] | null {
  const normalized = normalizeTaskOutputCursor(cursor);
  if (!normalized) return null;
  const [ms, sequence] = normalized.split("-").map(Number);
  return Number.isSafeInteger(ms) && Number.isSafeInteger(sequence)
    ? [ms, sequence]
    : null;
}

export function taskOutputCursorIsAfter(
  cursor: string | null | undefined,
  previous: string | null | undefined,
): boolean {
  const nextParts = taskOutputCursorParts(cursor);
  if (!nextParts) return false;
  const previousParts = taskOutputCursorParts(previous);
  if (!previousParts) return true;
  return nextParts[0] > previousParts[0] ||
    (nextParts[0] === previousParts[0] && nextParts[1] > previousParts[1]);
}

export function taskOutputCursorIsBefore(
  cursor: string | null | undefined,
  previous: string | null | undefined,
): boolean {
  const nextParts = taskOutputCursorParts(cursor);
  const previousParts = taskOutputCursorParts(previous);
  if (!nextParts || !previousParts) return false;
  return nextParts[0] < previousParts[0] ||
    (nextParts[0] === previousParts[0] && nextParts[1] < previousParts[1]);
}

export function normalizeTaskOutputMessage(value: unknown): TaskOutputMessage | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const message = value as Record<string, unknown>;
  if (!Array.isArray(message.lines) || typeof message.done !== "boolean") return null;

  const error = typeof message.error === "string" ? message.error.trim() : "";
  const lines = message.lines.filter((line): line is string => typeof line === "string");
  const lastId = typeof message.last_id === "string" ? message.last_id : null;
  const cursor = normalizeTaskOutputCursor(lastId);

  if (lines.length > 0 && !cursor) {
    return {
      lines: [],
      last_id: "0-0",
      done: true,
      error: TASK_OUTPUT_PROTOCOL_ERROR,
    };
  }
  if (lastId === null) return null;

  return {
    lines,
    last_id: cursor ?? "0-0",
    done: message.done,
    ...(error
      ? { error }
      : {}),
  };
}
