export interface LockInfo {
  lock_key: string;
  prefix: string | null;
  resource: string;
  resource_type: string;
  repo: string;
  resource_id: string;
  owner: string;
  ttl: number;
  task_id: string | null;
  pending_created_at: string | null;
  output_prefix?: string | null;
  output_prefix_unresolved?: boolean;
}

export interface ConsumerGroupInfo {
  stream: string;
  name: string;
  consumers: number;
  pending: number;
  lag: number | null;
}

export interface RecentResult {
  result_id: string;
  result_stream: string;
  entry_id: string;
  task_id: string;
  worker_id: string;
  status: string;
  repo: string | null;
  resource_type: string;
  resource_id: string;
  duration_seconds: number;
  summary: string;
  output_prefix?: string | null;
  output_prefix_unresolved?: boolean;
}

export interface DeadLetterEntry {
  dead_letter_id: string;
  dead_letter_stream: string;
  entry_id: string;
  task_id: string;
  task_type: string;
  repo: string;
  resource_type: string;
  resource_id: string;
  timestamp_ms: number | null;
  reason: string | null;
}

export interface QueuedTask {
  entry_id: string;
  task_id: string;
  task_type: string;
  prefix?: string | null;
  repo: string;
  resource_type: string;
  resource_id: string;
  created_at: string | null;
  stream: string;
}

export interface WorkerPoolActiveVm {
  vmid: string;
  started_at: string | null;
  age_seconds: number | null;
}

export interface WorkerPoolInfo {
  prefix: string;
  template_vmid: string | null;
  idle: string[];
  active: WorkerPoolActiveVm[];
  idle_count: number;
  active_count: number;
}

export interface DashboardPolicy {
  max_attempts: number;
  pending_task_ttl_seconds: number;
  lock_ttl_seconds: number;
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
  provider_health: Record<string, Record<string, number>>;
  worker_pool: WorkerPoolInfo[];
  degraded_sections: string[];
  dashboard_policy: DashboardPolicy;
}

export interface StuckTask {
  prefix: string | null;
  resource_type: string;
  repo: string | null;
  resource_id: string;
  reason: string;
  severity: "warning" | "critical";
  stream?: string | null;
  consumer_group?: string | null;
  entry_id?: string | null;
  task_id?: string | null;
  no_worker_consumers?: boolean;
}

export interface DashboardMessage {
  snapshot: SystemSnapshot;
  stuck_tasks: StuckTask[];
  workers: string[];
}

export interface TaskOutputMessage {
  lines: string[];
  last_id: string;
  done: boolean;
  error?: string;
}
