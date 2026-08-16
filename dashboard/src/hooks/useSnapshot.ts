import { useState, useEffect, useRef, useCallback } from "react";
import { addDashboardToken } from "../lib/authToken";
import {
  SNAPSHOT_PROTOCOL_ERROR,
  SNAPSHOT_RECONNECT_ABNORMAL_CLOSE_THRESHOLD,
  snapshotCloseErrorMessage,
  snapshotFetchedDate,
} from "../lib/connection";
import { normalizeDashboardMessage } from "../lib/snapshot";
import type { SystemSnapshot, StuckTask } from "../lib/types";

interface SnapshotState {
  snapshot: SystemSnapshot | null;
  stuckTasks: StuckTask[];
  workers: string[];
  connected: boolean;
  error: string | null;
  /**
   * Browser clock at message receipt. This is the ONLY clock snapshot staleness
   * is measured against, so the comparison stays self-consistent even when the
   * dashboard server's clock is skewed relative to the browser's.
   */
  lastUpdate: Date | null;
  /** Server-stamped `snapshot.fetched_at`. Display only — never used for freshness. */
  serverFetchedAt: Date | null;
}

export function useSnapshot(): SnapshotState {
  const [state, setState] = useState<SnapshotState>({
    snapshot: null,
    stuckTasks: [],
    workers: [],
    connected: false,
    error: null,
    lastUpdate: null,
    serverFetchedAt: null,
  });

  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const shouldReconnectRef = useRef(false);
  const hasReceivedSnapshotRef = useRef(false);

  const connect = useCallback(() => {
    if (!shouldReconnectRef.current || wsRef.current) return;

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const qs = addDashboardToken(new URLSearchParams());
    const qsStr = qs.toString();
    const ws = new WebSocket(`${protocol}//${window.location.host}/ws/snapshot${qsStr ? `?${qsStr}` : ""}`);
    wsRef.current = ws;
    let opened = false;

    ws.onopen = () => {
      if (wsRef.current !== ws) return;
      opened = true;
      setState((prev) => ({ ...prev, connected: true, error: null }));
    };

    ws.onmessage = (event) => {
      if (wsRef.current !== ws) return;
      // Stamp receipt on the browser clock *before* any parsing, so the
      // staleness comparison in ConnectionStatus (which uses Date.now()) never
      // mixes clocks with the server-generated snapshot.fetched_at.
      const receivedAt = new Date();
      try {
        const msg = normalizeDashboardMessage(JSON.parse(event.data));
        if (!msg) throw new Error("Malformed snapshot message");
        setState({
          snapshot: msg.snapshot,
          stuckTasks: msg.stuck_tasks,
          workers: msg.workers,
          connected: true,
          error: null,
          lastUpdate: receivedAt,
          serverFetchedAt: snapshotFetchedDate(msg.snapshot.fetched_at),
        });
        hasReceivedSnapshotRef.current = true;
        retryRef.current = 0;
      } catch {
        setState((prev) => ({
          ...prev,
          connected: false,
          error: SNAPSHOT_PROTOCOL_ERROR,
        }));
        ws.close(1000, "Dashboard snapshot protocol error");
      }
    };

    ws.onclose = (event) => {
      if (wsRef.current !== ws) return;

      setState((prev) => ({ ...prev, connected: false }));
      wsRef.current = null;

      if (!shouldReconnectRef.current) return;

      // Exponential backoff: 1s, 2s, 4s, 8s, ... max 30s
      const delay = Math.min(1000 * Math.pow(2, retryRef.current), 30000);
      const nextFailureCount = retryRef.current + 1;
      const error = snapshotCloseErrorMessage(
        event.code,
        nextFailureCount,
        opened && hasReceivedSnapshotRef.current
          ? SNAPSHOT_RECONNECT_ABNORMAL_CLOSE_THRESHOLD
          : undefined,
      );
      if (error) {
        setState((prev) => ({ ...prev, error }));
      }
      retryRef.current = nextFailureCount;
      retryTimerRef.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, []);

  useEffect(() => {
    shouldReconnectRef.current = true;
    connect();
    return () => {
      shouldReconnectRef.current = false;
      const ws = wsRef.current;
      wsRef.current = null;
      ws?.close();
      if (retryTimerRef.current) {
        clearTimeout(retryTimerRef.current);
        retryTimerRef.current = undefined;
      }
    };
  }, [connect]);

  return state;
}
