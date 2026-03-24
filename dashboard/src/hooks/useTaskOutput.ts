import { useState, useEffect, useRef, useCallback } from "react";
import type { TaskOutputMessage } from "../lib/types";

interface TaskOutputState {
  lines: string[];
  startIndex: number;
  connected: boolean;
  done: boolean;
}

const MAX_LINES = 5000;

export interface TaskOutputParams {
  workerId: string;
  taskId?: string;
}

export function useTaskOutput(params: TaskOutputParams | null): TaskOutputState {
  const [state, setState] = useState<TaskOutputState>({
    lines: [],
    startIndex: 0,
    connected: false,
    done: false,
  });

  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const doneRef = useRef(false);

  const connect = useCallback(() => {
    if (!params) return;

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const qs = new URLSearchParams({ worker_id: params.workerId });
    if (params.taskId) qs.set("task_id", params.taskId);
    const token = new URLSearchParams(window.location.search).get("token");
    if (token) qs.set("token", token);
    const ws = new WebSocket(
      `${protocol}//${window.location.host}/ws/task-output?${qs}`
    );
    wsRef.current = ws;

    ws.onopen = () => {
      retryRef.current = 0;
      setState((prev) => ({ ...prev, connected: true }));
    };

    ws.onmessage = (event) => {
      try {
        const msg: TaskOutputMessage = JSON.parse(event.data);
        if (msg.lines && msg.lines.length > 0) {
          setState((prev) => {
            const newLines = [...prev.lines, ...msg.lines];
            if (newLines.length > MAX_LINES) {
              const sliced = newLines.length - MAX_LINES;
              return {
                ...prev,
                lines: newLines.slice(sliced),
                startIndex: prev.startIndex + sliced,
                done: msg.done,
              };
            }
            return {
              ...prev,
              lines: newLines,
              done: msg.done,
            };
          });
          if (msg.done) doneRef.current = true;
        } else if (msg.done) {
          doneRef.current = true;
          setState((prev) => ({ ...prev, done: true }));
        }
      } catch {
        // ignore
      }
    };

    ws.onclose = () => {
      setState((prev) => ({ ...prev, connected: false }));
      wsRef.current = null;

      // Don't reconnect if task is done
      if (doneRef.current) return;

      const delay = Math.min(1000 * Math.pow(2, retryRef.current), 30000);
      retryRef.current++;
      retryTimerRef.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, [params?.workerId, params?.taskId]);

  useEffect(() => {
    setState({ lines: [], startIndex: 0, connected: false, done: false });
    doneRef.current = false;
    wsRef.current?.close();
    if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
    retryRef.current = 0;

    if (params) {
      connect();
    }

    return () => {
      wsRef.current?.close();
      if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
    };
  }, [params?.workerId, params?.taskId, connect]);

  return state;
}
