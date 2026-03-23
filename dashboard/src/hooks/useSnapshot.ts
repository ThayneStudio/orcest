import { useState, useEffect, useRef, useCallback } from "react";
import type { SystemSnapshot, StuckTask, DashboardMessage } from "../lib/types";

interface SnapshotState {
  snapshot: SystemSnapshot | null;
  stuckTasks: StuckTask[];
  workers: string[];
  connected: boolean;
  lastUpdate: Date | null;
}

export function useSnapshot(): SnapshotState {
  const [state, setState] = useState<SnapshotState>({
    snapshot: null,
    stuckTasks: [],
    workers: [],
    connected: false,
    lastUpdate: null,
  });

  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const connect = useCallback(() => {
    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const qs = new URLSearchParams();
    const token = new URLSearchParams(window.location.search).get("token");
    if (token) qs.set("token", token);
    const qsStr = qs.toString();
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/snapshot${qsStr ? `?${qsStr}` : ""}`);
    wsRef.current = ws;

    ws.onopen = () => {
      retryRef.current = 0;
      setState((prev) => ({ ...prev, connected: true }));
    };

    ws.onmessage = (event) => {
      try {
        const msg: DashboardMessage = JSON.parse(event.data);
        setState({
          snapshot: msg.snapshot,
          stuckTasks: msg.stuck_tasks,
          workers: msg.workers,
          connected: true,
          lastUpdate: new Date(),
        });
      } catch {
        // ignore malformed messages
      }
    };

    ws.onclose = () => {
      setState((prev) => ({ ...prev, connected: false }));
      wsRef.current = null;

      // Exponential backoff: 1s, 2s, 4s, 8s, ... max 30s
      const delay = Math.min(1000 * Math.pow(2, retryRef.current), 30000);
      retryRef.current++;
      retryTimerRef.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, []);

  useEffect(() => {
    connect();
    return () => {
      wsRef.current?.close();
      if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
    };
  }, [connect]);

  return state;
}
