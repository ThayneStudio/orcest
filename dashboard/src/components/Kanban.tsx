import { useEffect, useRef, useState } from "react";
import type { SystemSnapshot, StuckTask } from "../lib/types";
import { useTaskOutput, type TaskOutputParams } from "../hooks/useTaskOutput";
import { formatDuration, formatTtl, statusColor, timeAgo } from "../lib/format";

interface Props {
  snapshot: SystemSnapshot;
  stuckTasks: StuckTask[];
}

function taskTypeLabel(type: string): string {
  switch (type) {
    case "FIX_PR": return "Fix PR";
    case "FIX_CI": return "Fix CI";
    case "IMPLEMENT_ISSUE": return "Implement";
    case "TRIAGE_FOLLOWUPS": return "Triage";
    case "REBASE_PR": return "Rebase";
    case "IMPROVE": return "Improve";
    default: return type;
  }
}

function taskTypeBadge(type: string): string {
  switch (type) {
    case "FIX_PR":
    case "FIX_CI": return "bg-orange-500/20 text-orange-400";
    case "IMPLEMENT_ISSUE": return "bg-blue-500/20 text-blue-400";
    case "TRIAGE_FOLLOWUPS": return "bg-violet-500/20 text-violet-400";
    case "REBASE_PR": return "bg-cyan-500/20 text-cyan-400";
    default: return "bg-zinc-500/20 text-zinc-400";
  }
}

export function Kanban({ snapshot, stuckTasks }: Props) {
  const stuckIds = new Set(stuckTasks.map((t) => t.resource_id));
  const [selectedTask, setSelectedTask] = useState<TaskOutputParams | null>(null);
  const [selectedLabel, setSelectedLabel] = useState<string>("");

  const completed = snapshot.recent_results.filter(
    (r) => r.status.toLowerCase() === "completed"
  );
  const failed = snapshot.recent_results.filter(
    (r) => r.status.toLowerCase() !== "completed"
  );

  const columns = [
    { title: "Queued", color: "border-zinc-500", headerBg: "bg-zinc-800", count: snapshot.queued_tasks.length },
    { title: "In Progress", color: "border-blue-500", headerBg: "bg-blue-500/10", count: snapshot.locks.length },
    { title: "Completed", color: "border-emerald-500", headerBg: "bg-emerald-500/10", count: completed.length },
    { title: "Failed / Blocked", color: "border-red-500", headerBg: "bg-red-500/10", count: failed.length },
  ];

  const selectTask = (params: TaskOutputParams | null, label: string) => {
    if (selectedTask?.workerId === params?.workerId && selectedTask?.taskId === params?.taskId) {
      setSelectedTask(null);
      setSelectedLabel("");
    } else {
      setSelectedTask(params);
      setSelectedLabel(label);
    }
  };

  return (
    <div className="flex flex-col h-[calc(100vh-14rem)]">
      <div className={`flex gap-4 overflow-x-auto ${selectedTask ? "h-1/2" : "flex-1"} min-h-0`}>
        {/* Queued */}
        <Column header={columns[0]}>
          {snapshot.queued_tasks.length === 0 ? (
            <EmptyState>No tasks queued</EmptyState>
          ) : (
            snapshot.queued_tasks.map((task) => (
              <Card key={`${task.stream}-${task.task_id}`} className="opacity-75">
                <div className="flex items-center justify-between mb-2">
                  <span className="font-mono text-sm">
                    {task.resource_type} #{task.resource_id}
                  </span>
                  <span className={`text-xs rounded-full px-2 py-0.5 ${taskTypeBadge(task.task_type)}`}>
                    {taskTypeLabel(task.task_type)}
                  </span>
                </div>
                <div className="text-xs text-zinc-500 truncate" title={task.repo}>{task.repo}</div>
                <div className="flex items-center justify-between mt-2">
                  <span className="text-xs text-zinc-600 truncate" title={task.stream}>{task.stream}</span>
                  {task.created_at && <span className="text-xs text-zinc-600">{timeAgo(task.created_at)}</span>}
                </div>
              </Card>
            ))
          )}
        </Column>

        {/* In Progress */}
        <Column header={columns[1]}>
          {snapshot.locks.length === 0 ? (
            <EmptyState>No active work</EmptyState>
          ) : (
            snapshot.locks.map((lock) => {
              const lockNum = lock.resource.split(":").pop() || lock.resource;
              const isStuck = stuckIds.has(lockNum);
              const isSelected = selectedTask?.workerId === lock.owner && !selectedTask?.taskId;
              return (
                <Card
                  key={lock.resource}
                  className={`cursor-pointer transition-colors ${
                    isStuck ? "border-red-500/50 ring-1 ring-red-500/20" :
                    isSelected ? "border-blue-500/50 ring-1 ring-blue-500/30" : "hover:border-zinc-600"
                  }`}
                  onClick={() => selectTask(
                    { workerId: lock.owner },
                    `#${lock.resource}`
                  )}
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-mono text-sm">#{lock.resource}</span>
                    <div className="flex items-center gap-1.5">
                      {isStuck && (
                        <span className="text-xs rounded-full px-2 py-0.5 bg-red-500/20 text-red-400">Stuck</span>
                      )}
                      <span className="text-xs text-blue-400">View Output</span>
                    </div>
                  </div>
                  <div className="text-xs text-zinc-400">Worker: {lock.owner}</div>
                  <div className="text-xs text-zinc-500 mt-1">TTL: {formatTtl(lock.ttl)}</div>
                </Card>
              );
            })
          )}
        </Column>

        {/* Completed */}
        <Column header={columns[2]}>
          {completed.length === 0 ? (
            <EmptyState>No recent completions</EmptyState>
          ) : (
            completed.slice(0, 20).map((r) => {
              const isSelected = selectedTask?.workerId === r.worker_id && selectedTask?.taskId === r.task_id;
              return (
                <Card
                  key={r.task_id}
                  className={`cursor-pointer transition-colors ${
                    isSelected ? "border-emerald-500/50 ring-1 ring-emerald-500/30" : "hover:border-zinc-600"
                  }`}
                  onClick={() => selectTask(
                    { workerId: r.worker_id, taskId: r.task_id },
                    `${r.resource_type} #${r.resource_id}`
                  )}
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-mono text-sm">{r.resource_type} #{r.resource_id}</span>
                    <span className="text-xs font-mono text-zinc-500">{formatDuration(r.duration_seconds)}</span>
                  </div>
                  <div className="text-xs text-zinc-400 truncate">{r.worker_id}</div>
                  {r.summary && <div className="text-xs text-zinc-500 mt-1 line-clamp-2">{r.summary}</div>}
                  <div className="text-xs text-emerald-400/60 mt-1">View Output</div>
                </Card>
              );
            })
          )}
        </Column>

        {/* Failed / Blocked */}
        <Column header={columns[3]}>
          {failed.length === 0 ? (
            <EmptyState>No failures</EmptyState>
          ) : (
            failed.slice(0, 20).map((r) => {
              const isSelected = selectedTask?.workerId === r.worker_id && selectedTask?.taskId === r.task_id;
              return (
                <Card
                  key={r.task_id}
                  className={`cursor-pointer transition-colors border-red-500/20 ${
                    isSelected ? "ring-1 ring-red-500/30" : "hover:border-zinc-600"
                  }`}
                  onClick={() => selectTask(
                    { workerId: r.worker_id, taskId: r.task_id },
                    `${r.resource_type} #${r.resource_id}`
                  )}
                >
                  <div className="flex items-center justify-between mb-2">
                    <span className="font-mono text-sm">{r.resource_type} #{r.resource_id}</span>
                    <span className={`text-xs rounded-full px-2 py-0.5 ${statusColor(r.status)}`}>{r.status}</span>
                  </div>
                  <div className="text-xs text-zinc-400 truncate">{r.worker_id} - {formatDuration(r.duration_seconds)}</div>
                  {r.summary && <div className="text-xs text-red-400/80 mt-1 line-clamp-2">{r.summary}</div>}
                  <div className="text-xs text-red-400/60 mt-1">View Output</div>
                </Card>
              );
            })
          )}
        </Column>
      </div>

      {/* Task Output Panel */}
      {selectedTask && (
        <TaskOutputPanel
          params={selectedTask}
          label={selectedLabel}
          onClose={() => { setSelectedTask(null); setSelectedLabel(""); }}
        />
      )}
    </div>
  );
}

function TaskOutputPanel({
  params,
  label,
  onClose,
}: {
  params: TaskOutputParams;
  label: string;
  onClose: () => void;
}) {
  const { lines, startIndex, connected, done } = useTaskOutput(params);
  const logRef = useRef<HTMLDivElement>(null);
  const [autoScroll, setAutoScroll] = useState(true);

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
    <div className="flex flex-col h-1/2 border-t border-zinc-800 mt-3 pt-3">
      <div className="flex items-center justify-between mb-2 px-1">
        <div className="flex items-center gap-3">
          <span className="text-sm font-medium text-zinc-300">
            Output: {label}
          </span>
          <span className="text-xs text-zinc-500">
            worker: {params.workerId}
            {params.taskId && ` | task: ${params.taskId.slice(0, 8)}...`}
          </span>
          {connected && !done && (
            <span className="inline-flex items-center gap-1 text-xs text-emerald-400">
              <span className="h-1.5 w-1.5 rounded-full bg-emerald-400 animate-pulse" />
              Live
            </span>
          )}
          {done && (
            <span className="text-xs text-zinc-500">Complete</span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {!autoScroll && (
            <button
              className="text-xs text-zinc-400 hover:text-zinc-200 border border-zinc-700 rounded px-2 py-0.5"
              onClick={() => {
                setAutoScroll(true);
                if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
              }}
            >
              Scroll to bottom
            </button>
          )}
          <button
            className="text-xs text-zinc-400 hover:text-zinc-200 border border-zinc-700 rounded px-2 py-0.5"
            onClick={onClose}
          >
            Close
          </button>
        </div>
      </div>
      <div
        ref={logRef}
        onScroll={handleScroll}
        className="flex-1 min-h-0 overflow-auto rounded-lg border border-zinc-800 bg-zinc-950 p-4 font-mono text-xs leading-relaxed text-zinc-300"
      >
        {lines.length === 0 ? (
          <div className="text-zinc-600 italic">
            {connected ? "Waiting for output..." : "Connecting..."}
          </div>
        ) : (
          lines.map((line, i) => (
            <div
              key={startIndex + i}
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

function Column({
  header,
  children,
}: {
  header: { title: string; color: string; headerBg: string; count: number };
  children: React.ReactNode;
}) {
  return (
    <div className={`flex flex-col min-w-[280px] w-[280px] rounded-lg border border-zinc-800 ${header.color} border-t-2`}>
      <div className={`px-3 py-2.5 ${header.headerBg} rounded-t-lg flex items-center justify-between`}>
        <span className="text-sm font-medium">{header.title}</span>
        <span className="text-xs text-zinc-400 bg-zinc-800 rounded-full px-2 py-0.5">{header.count}</span>
      </div>
      <div className="flex-1 overflow-y-auto p-2 space-y-2">
        {children}
      </div>
    </div>
  );
}

function Card({
  children,
  className = "",
  onClick,
}: {
  children: React.ReactNode;
  className?: string;
  onClick?: () => void;
}) {
  return (
    <div
      className={`rounded-lg border border-zinc-800 bg-zinc-900 px-3 py-2.5 ${className}`}
      onClick={onClick}
    >
      {children}
    </div>
  );
}

function EmptyState({ children }: { children: React.ReactNode }) {
  return (
    <div className="text-xs text-zinc-600 italic text-center py-4">{children}</div>
  );
}
