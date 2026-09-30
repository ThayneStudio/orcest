import type { WorkerPoolInfo } from "./types.js";
export interface WorkAttempt {
  workerPrefix: string;
  taskId: string;
  workerId: string;
  provider: string;
  model: string;
  accountId: string;
  startedAt: number | null;
  finishedAt: number | null;
  status: string;
  outputPrefix: string;
}
export interface FleetWork {
  id: string;
  project: string;
  prefix: string;
  kind: "issue" | "pr";
  number: number;
  title: string;
  description: string;
  url: string;
  stage: "upcoming" | "in_progress" | "done" | "unknown";
  activity: string;
  reason: string;
  next: string;
  observedAt: number | null;
  startedAt: number | null;
  completedAt: number | null;
  stale: boolean;
  outcome: string | null;
  needsHuman: boolean;
  blockers: { label: string; url: string; workId: string | null }[];
  branch: string;
  headSha: string;
  relatedPr: string | null;
  publicationUrl?: string | null;
  latestAttempt: WorkAttempt | null;
  attempts?: WorkAttempt[];
}
export interface UsageObservation {
  observedAt: number;
  windows: { name: string; usedPercent: number; resetsAt: string | null }[];
}
export interface FleetAccount {
  usage: UsageObservation | null;
  id: string;
  provider: string;
  models: string[];
  projects: string[];
  availability: string;
  resetsAt: number | null;
  observedAt: number | null;
}
export interface FleetWorker {
  id: string;
  prefix: string;
  backend: string;
  revision: string;
  ttl: number;
  workId: string | null;
}
export interface WorkView {
  pools: WorkerPoolInfo[];
  version: 1;
  fetchedAt: number;
  items: FleetWork[];
  total: number;
  nextOffset: number | null;
  projects: string[];
  accounts: FleetAccount[];
  workers: FleetWorker[];
  coverage: "complete" | "partial" | "unavailable";
  notices: string[];
  counts: {
    upcoming: number;
    in_progress: number;
    done: number;
    unknown: number;
    queued: number;
    waiting: number;
    needsHuman: number;
    running: number;
  };
}
