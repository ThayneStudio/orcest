import { useEffect, useRef, useState } from "react";
import { useWorkerOutput } from "../hooks/useWorkerOutput";

interface Props {
  workers: string[];
}

export function WorkerOutput({ workers }: Props) {
  const [selectedWorker, setSelectedWorker] = useState<string | null>(
    workers[0] || null
  );
  const { lines, connected } = useWorkerOutput(selectedWorker);
  const logRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

  // Update selection if current worker disappears
  useEffect(() => {
    if (selectedWorker && !workers.includes(selectedWorker)) {
      setSelectedWorker(workers[0] || null);
    }
  }, [workers, selectedWorker]);

  // Auto-scroll to bottom
  useEffect(() => {
    if (autoScroll && logRef.current) {
      logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  }, [lines, autoScroll]);

  const handleScroll = () => {
    if (!logRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = logRef.current;
    // If user scrolled up more than 50px from bottom, disable auto-scroll
    setAutoScroll(scrollHeight - scrollTop - clientHeight < 50);
  };

  return (
    <div className="flex flex-col h-full">
      <div className="flex items-center gap-4 mb-3">
        <h2 className="text-sm font-medium text-zinc-400">Worker Output</h2>
        <select
          className="rounded border border-zinc-700 bg-zinc-800 px-3 py-1.5 text-sm text-zinc-200 focus:outline-none focus:border-zinc-500"
          value={selectedWorker || ""}
          onChange={(e) => setSelectedWorker(e.target.value || null)}
        >
          {workers.length === 0 ? (
            <option value="">No workers available</option>
          ) : (
            workers.map((w) => (
              <option key={w} value={w}>
                {w}
              </option>
            ))
          )}
        </select>
        {selectedWorker && (
          <span className="text-xs text-zinc-500">
            {connected ? (
              <span className="text-emerald-400">streaming</span>
            ) : (
              <span className="text-zinc-500">connecting...</span>
            )}
          </span>
        )}
        {!autoScroll && (
          <button
            className="text-xs text-zinc-400 hover:text-zinc-200 border border-zinc-700 rounded px-2 py-0.5"
            onClick={() => {
              setAutoScroll(true);
              if (logRef.current) {
                logRef.current.scrollTop = logRef.current.scrollHeight;
              }
            }}
          >
            Scroll to bottom
          </button>
        )}
      </div>
      <div
        ref={logRef}
        onScroll={handleScroll}
        className="flex-1 min-h-0 overflow-auto rounded-lg border border-zinc-800 bg-zinc-950 p-4 font-mono text-xs leading-relaxed text-zinc-300"
      >
        {lines.length === 0 ? (
          <div className="text-zinc-600 italic">
            {selectedWorker
              ? "Waiting for output..."
              : "Select a worker to view output"}
          </div>
        ) : (
          lines.map((line, i) => (
            <div
              key={i}
              className={
                line.startsWith("───")
                  ? "text-cyan-400 font-bold border-t border-zinc-800 pt-2 mt-2"
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
