import type { DashboardMessage, SystemSnapshot, StuckTask } from "./types.js";
import { fetchSnapshot } from "./snapshot.js";
import { detectStuck } from "./stuck.js";
import { discoverWorkers, WorkerDiscoveryPartialError } from "./workers.js";

type DashboardMessageDeps = {
  fetchSnapshot?: () => Promise<SystemSnapshot>;
  detectStuck?: (snapshot: SystemSnapshot) => Promise<StuckTask[]>;
  discoverWorkers?: () => Promise<string[]>;
  logError?: (message: string, err: unknown) => void;
};

export async function buildDashboardMessage(
  deps: DashboardMessageDeps = {},
): Promise<DashboardMessage> {
  const loadSnapshot = deps.fetchSnapshot ?? fetchSnapshot;
  const loadStuck = deps.detectStuck ?? detectStuck;
  const loadWorkers = deps.discoverWorkers ?? discoverWorkers;
  const logError = deps.logError ?? console.error;

  const snapshot = await loadSnapshot();
  if (!snapshot.redis_ok) {
    return { snapshot, stuck_tasks: [], workers: [] };
  }

  let stuckDegraded = false;
  let workerDiscoveryDegraded = false;
  const [stuckTasks, workers] = await Promise.all([
    loadStuck(snapshot).catch((err) => {
      logError("Error detecting stuck tasks:", err);
      stuckDegraded = true;
      return [];
    }),
    loadWorkers().catch((err) => {
      logError("Error discovering workers:", err);
      workerDiscoveryDegraded = true;
      if (err instanceof WorkerDiscoveryPartialError) {
        return err.workers;
      }
      return [];
    }),
  ]);
  if (stuckDegraded) markSnapshotDegraded(snapshot, "stuck tasks");
  if (workerDiscoveryDegraded) markSnapshotDegraded(snapshot, "worker discovery");

  return { snapshot, stuck_tasks: stuckTasks, workers };
}

function markSnapshotDegraded(snapshot: SystemSnapshot, section: string): void {
  if (!snapshot.degraded_sections.includes(section)) {
    snapshot.degraded_sections.push(section);
  }
}
