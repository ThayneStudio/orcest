import type { ConsumerGroupInfo, QueuedTask, SystemSnapshot, WorkerPoolActiveVm } from "../lib/types";
import { formatDuration } from "../lib/format";
import { degradedCountLabel } from "../lib/counts";
import {
  consumerGroupBacklogEvidence,
  consumerGroupHasNoConsumersWhileBacklogged,
  consumerGroupQueueDepth,
  consumerGroupQueuedPreviewCount,
  queuedPreviewCountsByStream,
} from "../lib/consumerGroups";
import { queueStreamDisplayName } from "../lib/queues";

interface Props {
  pools: SystemSnapshot["worker_pool"];
  workers: string[];
  consumerGroups?: ConsumerGroupInfo[];
  queueDepths?: Record<string, number>;
  queuedTasks?: QueuedTask[];
  poolDegraded?: boolean;
  workerDiscoveryDegraded?: boolean;
}

function formatPoolName(prefix: string): string {
  return prefix || "default";
}

function formatAge(seconds: number | null): string {
  return seconds === null ? "unknown" : formatDuration(seconds);
}

export function workerVmListEmptyMessage(degraded: boolean, reportedCount = 0): string {
  if (degraded) return "unavailable";
  return reportedCount > 0 ? "details unavailable" : "none";
}

function VmChips({
  vmids,
  degraded,
  reportedCount,
}: {
  vmids: string[];
  degraded: boolean;
  reportedCount: number;
}) {
  if (vmids.length === 0) {
    return <div className="text-sm text-zinc-600">{workerVmListEmptyMessage(degraded, reportedCount)}</div>;
  }

  return (
    <div className="flex max-h-24 flex-wrap gap-1.5 overflow-y-auto pr-1">
      {vmids.map((vmid) => (
        <span
          key={vmid}
          className="max-w-full break-all rounded-md border border-zinc-700 bg-zinc-950 px-2 py-0.5 font-mono text-xs text-zinc-300"
          title={vmid}
        >
          {vmid}
        </span>
      ))}
    </div>
  );
}

export function workerOutputStreamsEmptyMessage(degraded: boolean): string {
  return degraded ? "worker discovery unavailable" : "no worker output in the last 7 days";
}

export function workerPoolEmptyMessage(degraded: boolean): string {
  return degraded
    ? "VM pool state unavailable. Recent worker output is activity evidence only."
    : "No VM pool state reported. Recent worker output is activity evidence only.";
}

export function workerRuntimeCountLabel(count: number, degraded: boolean): string {
  return degradedCountLabel(count, degraded);
}

export function workerRuntimeUnitLabel(count: number): string {
  return count === 1 ? "worker" : "workers";
}

export function workerDiscoveryStatusText(count: number, degraded: boolean): string | null {
  if (!degraded) return null;
  return count > 0 ? "partial worker discovery" : "worker discovery unavailable";
}

export function workerPoolStatusText(degraded: boolean, hasPools: boolean): string | null {
  if (!degraded || !hasPools) return null;
  return "VM pool data may be incomplete";
}

export function workerConsumerCoverageLabel(
  groups: ConsumerGroupInfo[] = [],
  queueDepths: Record<string, number> = {},
  queuedTasks: QueuedTask[] = [],
): string | null {
  const queuedPreviews = queuedPreviewCountsByStream(queuedTasks);
  const entriesByStream = new Map<string, { stream: string; count: number; exact: boolean }>();
  for (const group of groups) {
    const stream = group.stream.trim();
    if (!stream) continue;
    const queueDepth = consumerGroupQueueDepth(group, queueDepths);
    const queuedPreview = consumerGroupQueuedPreviewCount(group, queuedPreviews);
    if (!consumerGroupHasNoConsumersWhileBacklogged(group, queueDepth, queuedPreview)) continue;

    const evidence = consumerGroupBacklogEvidence(group, { queueDepth, queuedPreview });
    const existing = entriesByStream.get(stream);
    if (!existing || evidence.count > existing.count) {
      entriesByStream.set(stream, { stream, count: evidence.count, exact: evidence.exact });
    } else if (existing && evidence.count === existing.count && !evidence.exact) {
      existing.exact = false;
    }
  }

  const entries = [...entriesByStream.values()].sort((a, b) =>
    b.count - a.count || queueStreamDisplayName(a.stream).localeCompare(queueStreamDisplayName(b.stream))
  );
  if (entries.length === 0) return null;

  const names = entries.map((entry) => queueStreamDisplayName(entry.stream));
  const total = entries.reduce((sum, entry) => sum + entry.count, 0);
  const exact = entries.every((entry) => entry.exact);
  const backlogLabel =
    `${exact ? "" : "at least "}${total} queued/pending ${total === 1 ? "item" : "items"}`;
  const verb = entries.length === 1 ? "has" : "have";
  if (names.length <= 2) return `${names.join(" and ")} ${verb} ${backlogLabel} with no consumers`;
  return `${names[0]}, ${names[1]}, and ${names.length - 2} more ${verb} ${backlogLabel} with no consumers`;
}

function degradedCardClass(degraded: boolean): string {
  return degraded ? "border-yellow-500/30" : "border-zinc-800";
}

function countToneClass(degraded: boolean, healthyClass: string): string {
  return degraded ? "text-yellow-300" : healthyClass;
}

export const WORKER_RUNTIME_COUNT_CLASS =
  "min-w-0 max-w-[8rem] break-all font-mono text-lg font-semibold";

function WorkerChips({ workers, degraded }: { workers: string[]; degraded: boolean }) {
  if (workers.length === 0) {
    return <div className="text-sm text-zinc-600">{workerOutputStreamsEmptyMessage(degraded)}</div>;
  }

  return (
    <div className="flex max-h-28 flex-wrap gap-1.5 overflow-y-auto pr-1">
      {workers.map((worker) => (
        <span
          key={worker}
          className="max-w-full break-all rounded-md border border-zinc-700 bg-zinc-950 px-2 py-0.5 font-mono text-xs text-zinc-300"
          title={worker}
        >
          {worker}
        </span>
      ))}
    </div>
  );
}

function ActiveVmRows({
  active,
  degraded,
  reportedCount,
}: {
  active: WorkerPoolActiveVm[];
  degraded: boolean;
  reportedCount: number;
}) {
  if (active.length === 0) {
    return <div className="text-sm text-zinc-600">{workerVmListEmptyMessage(degraded, reportedCount)}</div>;
  }

  return (
    <div className="max-h-32 space-y-1 overflow-y-auto pr-1">
      {active.map((vm) => (
        <div
          key={vm.vmid}
          className="flex min-w-0 items-center justify-between gap-3 rounded-md border border-zinc-800 bg-zinc-950 px-2 py-1 text-sm"
          title={vm.started_at || undefined}
        >
          <span className="min-w-0 break-all font-mono text-zinc-200">{vm.vmid}</span>
          <span className="shrink-0 font-mono text-xs text-zinc-500">
            {formatAge(vm.age_seconds)}
          </span>
        </div>
      ))}
    </div>
  );
}

export function WorkerPool({
  pools,
  workers,
  consumerGroups = [],
  queueDepths = {},
  queuedTasks = [],
  poolDegraded = false,
  workerDiscoveryDegraded = false,
}: Props) {
  const hasPools = pools.length > 0;
  const discoveryStatus = workerDiscoveryStatusText(workers.length, workerDiscoveryDegraded);
  const poolStatus = workerPoolStatusText(poolDegraded, hasPools);
  const consumerCoverageLabel = workerConsumerCoverageLabel(consumerGroups, queueDepths, queuedTasks);

  return (
    <div>
      <h2 className="mb-3 text-sm font-medium text-zinc-400">Worker Runtime</h2>
      <div className="grid grid-cols-1 gap-3 xl:grid-cols-2">
        <div className={`rounded-lg border bg-zinc-900 px-4 py-3 ${degradedCardClass(workerDiscoveryDegraded)}`}>
          <div className="mb-3 flex items-start justify-between gap-3">
            <div>
              <div className="text-sm font-medium text-zinc-200">
                Recent Worker Output
              </div>
              <div className="mt-1 text-xs text-zinc-500">
                worker IDs with output in the last 7 days, not current liveness
              </div>
              {discoveryStatus && (
                <div className="mt-1 text-xs text-yellow-300">
                  {discoveryStatus}
                </div>
              )}
              {consumerCoverageLabel && (
                <div className="mt-1 text-xs text-red-300">
                  {consumerCoverageLabel}
                </div>
              )}
            </div>
            <div className="text-right">
              <div
                className={`${WORKER_RUNTIME_COUNT_CLASS} ${countToneClass(workerDiscoveryDegraded, "text-sky-300")}`}
                title={workerRuntimeCountLabel(workers.length, workerDiscoveryDegraded)}
              >
                {workerRuntimeCountLabel(workers.length, workerDiscoveryDegraded)}
              </div>
              <div className="text-xs text-zinc-500">
                {workerRuntimeUnitLabel(workers.length)}
              </div>
            </div>
          </div>
          <WorkerChips workers={workers} degraded={workerDiscoveryDegraded} />
        </div>

        {!hasPools && (
          <div className="rounded-lg border border-zinc-800 bg-zinc-900 px-4 py-3 text-sm text-zinc-500">
            {workerPoolEmptyMessage(poolDegraded)}
          </div>
        )}

        {hasPools && (
          <>
            {pools.map((pool) => (
              <div
                key={pool.prefix || "__default__"}
                className={`min-w-0 rounded-lg border bg-zinc-900 px-4 py-3 ${degradedCardClass(poolDegraded)}`}
              >
                <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className="break-all font-mono text-sm text-zinc-200">
                      {formatPoolName(pool.prefix)}
                    </div>
                    <div className="mt-1 text-xs text-zinc-500">
                      template{" "}
                      <span className="break-all font-mono text-zinc-300">
                        {pool.template_vmid || "not set"}
                      </span>
                    </div>
                    {poolStatus && (
                      <div className="mt-1 text-xs text-yellow-300">
                        {poolStatus}
                      </div>
                    )}
                  </div>
                  <div className="flex min-w-0 gap-4 text-right">
                    <div>
                      <div
                        className={`${WORKER_RUNTIME_COUNT_CLASS} ${countToneClass(poolDegraded, "text-emerald-300")}`}
                        title={workerRuntimeCountLabel(pool.idle_count, poolDegraded)}
                      >
                        {workerRuntimeCountLabel(pool.idle_count, poolDegraded)}
                      </div>
                      <div className="text-xs text-zinc-500">idle</div>
                    </div>
                    <div>
                      <div
                        className={`${WORKER_RUNTIME_COUNT_CLASS} ${countToneClass(poolDegraded, "text-sky-300")}`}
                        title={workerRuntimeCountLabel(pool.active_count, poolDegraded)}
                      >
                        {workerRuntimeCountLabel(pool.active_count, poolDegraded)}
                      </div>
                      <div className="text-xs text-zinc-500">active</div>
                    </div>
                  </div>
                </div>

                <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
                  <div>
                    <div className="mb-1.5 text-xs text-zinc-600">
                      Idle VMIDs
                    </div>
                    <VmChips
                      vmids={pool.idle}
                      degraded={poolDegraded}
                      reportedCount={pool.idle_count}
                    />
                  </div>
                  <div>
                    <div className="mb-1.5 text-xs text-zinc-600">
                      Active VMIDs
                    </div>
                    <ActiveVmRows
                      active={pool.active}
                      degraded={poolDegraded}
                      reportedCount={pool.active_count}
                    />
                  </div>
                </div>
              </div>
            ))}
          </>
        )}
      </div>
    </div>
  );
}
