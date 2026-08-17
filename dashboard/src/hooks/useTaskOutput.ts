import { useState, useEffect, useRef, useCallback } from "react";
import { addDashboardToken } from "../lib/authToken";
import type { TaskOutputParams } from "../lib/types";
import {
  applyTaskOutputMessage,
  normalizeTaskOutputCursor,
  normalizeTaskOutputMessage,
  TASK_OUTPUT_MALFORMED_MESSAGE_ERROR,
  TASK_OUTPUT_MAX_LINES,
  TASK_OUTPUT_PROTOCOL_ERROR,
  taskOutputCursorIsBefore,
  taskOutputCursorIsAfter,
  taskOutputMessageMarksDone,
  taskOutputCloseStatus,
  type TaskOutputClientState,
  taskOutputTerminalError,
} from "../lib/taskOutput";
import { abnormalCloseMessage } from "../lib/websocket";

const TASK_OUTPUT_ABNORMAL_CLOSE_THRESHOLD = 5;

export function useTaskOutput(params: TaskOutputParams | null): TaskOutputClientState {
  const workerId = params?.workerId.trim() || "";
  const taskId = params?.taskId?.trim() || "";
  const historical = Boolean(params?.historical);
  const rawRedisPrefix = params?.prefix;
  const redisPrefix = typeof rawRedisPrefix === "string"
    ? (rawRedisPrefix.trim() || null)
    : rawRedisPrefix;
  const hasParams = params !== null && workerId !== "";
  const hasRedisPrefix = params !== null && rawRedisPrefix !== undefined;
  const paramsKey = hasParams
    ? [
      redisPrefix === undefined ? "*" : redisPrefix ?? "",
      workerId,
      taskId,
      historical ? "1" : "0",
    ].join("\u0000")
    : "";
  const [state, setState] = useState<TaskOutputClientState>({
    lines: [],
    startIndex: 0,
    connected: false,
    retrying: false,
    done: false,
    error: null,
  });

  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const doneRef = useRef(false);
  const shouldReconnectRef = useRef(false);
  const activeParamsKeyRef = useRef("");
  const lastIdRef = useRef("0-0");

  const connect = useCallback(() => {
    if (!hasParams || !shouldReconnectRef.current || activeParamsKeyRef.current !== paramsKey) {
      return;
    }

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const qs = new URLSearchParams({ worker_id: workerId });
    if (taskId) qs.set("task_id", taskId);
    if (hasRedisPrefix) qs.set("redis_prefix", redisPrefix ?? "");
    if (historical) qs.set("historical", "1");
    const afterId = normalizeTaskOutputCursor(lastIdRef.current);
    if (afterId) qs.set("after_id", afterId);
    addDashboardToken(qs);
    const ws = new WebSocket(
      `${protocol}//${window.location.host}/ws/task-output?${qs}`
    );
    const wsParamsKey = paramsKey;
    wsRef.current = ws;

    ws.onopen = () => {
      if (wsRef.current !== ws || activeParamsKeyRef.current !== wsParamsKey) return;
      setState((prev) => ({
        ...prev,
        connected: true,
        retrying: false,
        error: null,
      }));
    };

    ws.onmessage = (event) => {
      if (wsRef.current !== ws || activeParamsKeyRef.current !== wsParamsKey) return;
      if (doneRef.current) return;
      const stopWithProtocolError = (error: string) => {
        doneRef.current = true;
        setState((prev) => ({
          ...prev,
          connected: false,
          retrying: false,
          done: true,
          error,
        }));
        ws.close(1000, "Task output protocol error");
      };

      try {
        const msg = normalizeTaskOutputMessage(JSON.parse(event.data));
        if (!msg) {
          stopWithProtocolError(TASK_OUTPUT_MALFORMED_MESSAGE_ERROR);
          return;
        }
        const cursor = normalizeTaskOutputCursor(msg.last_id);
        if (cursor && taskOutputCursorIsBefore(cursor, lastIdRef.current)) {
          return;
        }
        const cursorAdvanced = Boolean(
          cursor && taskOutputCursorIsAfter(cursor, lastIdRef.current),
        );
        const marksDone = taskOutputMessageMarksDone(msg);
        if (msg.lines.length > 0 && cursor && !cursorAdvanced && !marksDone) {
          return;
        }
        if (cursorAdvanced && cursor) {
          lastIdRef.current = cursor;
        }
        if (cursorAdvanced || msg.lines.length > 0 || marksDone) {
          retryRef.current = 0;
        }
        if (marksDone) {
          doneRef.current = true;
        }
        setState((prev) => applyTaskOutputMessage(prev, msg, TASK_OUTPUT_MAX_LINES));
        if (msg.error === TASK_OUTPUT_PROTOCOL_ERROR) {
          ws.close(1000, "Task output protocol error");
        }
      } catch {
        stopWithProtocolError(TASK_OUTPUT_MALFORMED_MESSAGE_ERROR);
      }
    };

    ws.onclose = (event) => {
      if (wsRef.current !== ws || activeParamsKeyRef.current !== wsParamsKey) return;

      setState((prev) => ({ ...prev, connected: false }));
      wsRef.current = null;

      if (!shouldReconnectRef.current) return;

      const closeStatus = taskOutputCloseStatus(event.code, event.reason);
      if (closeStatus.terminal) {
        doneRef.current = true;
        setState((prev) => ({
          ...prev,
          retrying: false,
          done: true,
          error: taskOutputTerminalError(prev.error, closeStatus.error),
        }));
        return;
      }

      // Don't reconnect if task is done
      if (doneRef.current) return;

      const nextFailureCount = retryRef.current + 1;
      const abnormalError = abnormalCloseMessage(
        event.code,
        nextFailureCount,
        TASK_OUTPUT_ABNORMAL_CLOSE_THRESHOLD,
        "Task output",
      );
      if (abnormalError && historical) {
        doneRef.current = true;
        setState((prev) => ({
          ...prev,
          retrying: false,
          done: true,
          error: abnormalError,
        }));
        return;
      }

      const delay = Math.min(1000 * Math.pow(2, retryRef.current), 30000);
      retryRef.current = nextFailureCount;
      setState((prev) => ({
        ...prev,
        retrying: true,
        error: abnormalError || prev.error,
      }));
      retryTimerRef.current = setTimeout(() => {
        if (activeParamsKeyRef.current === wsParamsKey) connect();
      }, delay);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, [hasParams, hasRedisPrefix, historical, paramsKey, redisPrefix, taskId, workerId]);

  useEffect(() => {
    setState({
      lines: [],
      startIndex: 0,
      connected: false,
      retrying: false,
      done: false,
      error: null,
    });
    activeParamsKeyRef.current = paramsKey;
    shouldReconnectRef.current = false;
    doneRef.current = false;
    lastIdRef.current = "0-0";
    const ws = wsRef.current;
    wsRef.current = null;
    ws?.close();
    if (retryTimerRef.current) {
      clearTimeout(retryTimerRef.current);
      retryTimerRef.current = undefined;
    }
    retryRef.current = 0;

    if (hasParams) {
      shouldReconnectRef.current = true;
      connect();
    }

    return () => {
      shouldReconnectRef.current = false;
      activeParamsKeyRef.current = "";
      const ws = wsRef.current;
      wsRef.current = null;
      ws?.close();
      if (retryTimerRef.current) {
        clearTimeout(retryTimerRef.current);
        retryTimerRef.current = undefined;
      }
    };
  }, [hasParams, paramsKey, connect]);

  return state;
}
