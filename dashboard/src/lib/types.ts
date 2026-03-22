export interface LockInfo {
  resource: string;
  owner: string;
  ttl: number;
}

export interface ConsumerGroupInfo {
  stream: string;
  name: string;
  consumers: number;
  pending: number;
}

export interface RecentResult {
  task_id: string;
  worker_id: string;
  status: string;
  resource_type: string;
  resource_id: string;
  duration_seconds: number;
  summary: string;
}

export interface DeadLetterEntry {
  entry_id: string;
  task_type: string;
  repo: string;
  resource_type: string;
  resource_id: string;
  timestamp_ms: number | null;
  reason: string | null;
}

export interface QueuedTask {
  task_id: string;
  task_type: string;
  repo: string;
  resource_type: string;
  resource_id: string;
  created_at: string | null;
  stream: string;
}

export interface SystemSnapshot {
  redis_ok: boolean;
  fetched_at: string;
  queue_depths: Record<string, number>;
  results_depth: number;
  dead_letter_count: number;
  locks: LockInfo[];
  consumer_groups: ConsumerGroupInfo[];
  recent_results: RecentResult[];
  attempt_counts: Record<string, number>;
  dead_letter_entries: DeadLetterEntry[];
  queued_tasks: QueuedTask[];
}

export interface StuckTask {
  resource_type: string;
  resource_id: string;
  reason: string;
  severity: "warning" | "critical";
}

export interface DashboardMessage {
  snapshot: SystemSnapshot;
  stuck_tasks: StuckTask[];
  workers: string[];
}

export interface WorkerOutputMessage {
  lines: string[];
  last_id: string;
}

export interface TaskOutputMessage {
  lines: string[];
  last_id: string;
  done: boolean;
}
