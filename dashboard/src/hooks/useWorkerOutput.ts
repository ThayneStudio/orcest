import { useState, useEffect, useRef, useCallback } from "react";

interface WorkerOutputState {
  lines: string[];
  connected: boolean;
}

const MAX_LINES = 5000;

export function useWorkerOutput(workerId: string | null): WorkerOutputState {
  const [state, setState] = useState<WorkerOutputState>({
    lines: [],
    connected: false,
  });

  const wsRef = useRef<WebSocket | null>(null);
  const retryRef = useRef(0);
  const retryTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

  const connect = useCallback(() => {
    if (!workerId) return;

    const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
    const ws = new WebSocket(
      `${protocol}//${window.location.host}/ws/worker?id=${encodeURIComponent(workerId)}`
    );
    wsRef.current = ws;

    ws.onopen = () => {
      retryRef.current = 0;
      setState((prev) => ({ ...prev, connected: true }));
    };

    ws.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.lines && Array.isArray(msg.lines)) {
          setState((prev) => {
            const newLines = [...prev.lines, ...msg.lines];
            // Ring buffer: keep last MAX_LINES
            return {
              ...prev,
              lines: newLines.length > MAX_LINES
                ? newLines.slice(newLines.length - MAX_LINES)
                : newLines,
            };
          });
        }
      } catch {
        // ignore
      }
    };

    ws.onclose = () => {
      setState((prev) => ({ ...prev, connected: false }));
      wsRef.current = null;

      const delay = Math.min(1000 * Math.pow(2, retryRef.current), 30000);
      retryRef.current++;
      retryTimerRef.current = setTimeout(connect, delay);
    };

    ws.onerror = () => {
      ws.close();
    };
  }, [workerId]);

  useEffect(() => {
    // Reset on worker change
    setState({ lines: [], connected: false });
    wsRef.current?.close();
    if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
    retryRef.current = 0;

    if (workerId) {
      connect();
    }

    return () => {
      wsRef.current?.close();
      if (retryTimerRef.current) clearTimeout(retryTimerRef.current);
    };
  }, [workerId, connect]);

  return state;
}
