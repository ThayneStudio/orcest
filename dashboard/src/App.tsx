import { useEffect, useState, type KeyboardEvent } from "react";
import { useSnapshot } from "./hooks/useSnapshot";
import { ConnectionStatus } from "./components/ConnectionStatus";
import {
  StuckAlerts,
  groupedStuckTasks,
  stuckTaskGroupIsNoConsumerBacklog,
} from "./components/StuckAlerts";
import { QueueDepths } from "./components/QueueDepths";
import { ActiveWork } from "./components/ActiveWork";
import { ConsumerGroups } from "./components/ConsumerGroups";
import { RecentResults, type RecentResultFilter } from "./components/RecentResults";
import { DeadLetters } from "./components/DeadLetters";
import { Kanban } from "./components/Kanban";
import { ProviderHealth } from "./components/ProviderHealth";
import { WorkerPool } from "./components/WorkerPool";
import { AttemptCounts } from "./components/AttemptCounts";
import { SnapshotDegraded } from "./components/SnapshotDegraded";
import { ResultHealth } from "./components/ResultHealth";
import { bootstrapDashboardAuthCookie } from "./lib/authToken";
import {
  consumerGroupNoConsumerBacklogCount,
  queuedPreviewCountsByStream,
} from "./lib/consumerGroups";
import { hasDegradedSection, normalizedDegradedSectionSet } from "./lib/degradedSections";
import { resultColumnForStatus } from "./lib/results";
import type { RecentResult, StuckTask } from "./lib/types";

export type Tab = "overview" | "kanban" | "results" | "dead-letters";

const TABS: { id: Tab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "kanban", label: "Kanban" },
  { id: "results", label: "Results" },
  { id: "dead-letters", label: "Dead Letters" },
];
const RECENT_RESULT_FILTERS: RecentResultFilter[] = ["all", "failed", "completed", "neutral"];
const RESULT_FILTER_PARAM = "result_filter";

export function dashboardTabId(tab: Tab): string {
  return `dashboard-tab-${tab}`;
}

export function dashboardTabPanelId(tab: Tab): string {
  return `dashboard-panel-${tab}`;
}

export function dashboardTabControls(tab: Tab): string {
  return dashboardTabPanelId(tab);
}

export function dashboardTabPanelClassName(
  activeTab: Tab,
  redisOk: boolean | undefined,
): string {
  if (!redisOk) return "";
  return activeTab === "kanban"
    ? "flex min-h-0 flex-1 flex-col gap-6"
    : "space-y-6";
}

export function nextDashboardTab(current: Tab, key: string): Tab | null {
  const index = TABS.findIndex((tab) => tab.id === current);
  if (index < 0) return null;

  switch (key) {
    case "ArrowRight":
      return TABS[(index + 1) % TABS.length].id;
    case "ArrowLeft":
      return TABS[(index - 1 + TABS.length) % TABS.length].id;
    case "Home":
      return TABS[0].id;
    case "End":
      return TABS[TABS.length - 1].id;
    default:
      return null;
  }
}

export function deadLetterTabBadgeText(
  count: number,
  depthDegraded: boolean,
  loaded = 0,
): string | null {
  const knownCount = Math.max(count, loaded);
  if (depthDegraded) return knownCount > 0 ? `${knownCount}+` : "?";
  return count > 0 ? String(count) : null;
}

export function deadLetterTabBadgeSrText(
  count: number,
  depthDegraded: boolean,
  loaded = 0,
): string {
  const knownCount = Math.max(count, loaded);
  if (depthDegraded) {
    return knownCount > 0
      ? `at least ${knownCount} dead-letter ${knownCount === 1 ? "entry" : "entries"}, total unknown`
      : "unknown dead-letter count";
  }
  return count === 1 ? "dead-letter entry" : "dead-letter entries";
}

export function resultsTabAttentionCount(
  results: Array<Pick<RecentResult, "status">>,
): number {
  return results.filter((result) => resultColumnForStatus(result.status) === "failed").length;
}

export function resultsTabBadgeText(
  attentionCount: number,
  resultsDegraded: boolean,
  resultsWindowIncomplete = false,
): string | null {
  if (attentionCount > 0) {
    return resultsDegraded || resultsWindowIncomplete ? `${attentionCount}+` : String(attentionCount);
  }
  return resultsDegraded || resultsWindowIncomplete ? "?" : null;
}

export function resultsTabBadgeSrText(
  attentionCount: number,
  resultsDegraded: boolean,
  resultsWindowIncomplete = false,
): string {
  if ((resultsDegraded || resultsWindowIncomplete) && attentionCount === 0) {
    return "unknown recent results needing attention";
  }
  const noun = attentionCount === 1 ? "result needs" : "results need";
  return `${resultsDegraded || resultsWindowIncomplete ? "loaded " : ""}recent ${noun} attention`;
}

export function overviewStuckBadgeText(
  criticalCount: number,
  warningCount: number,
  stuckDegraded = false,
  noConsumerQueueCount = 0,
): string | null {
  if (criticalCount > 0) return stuckDegraded ? `${criticalCount}+` : String(criticalCount);
  if (noConsumerQueueCount > 0) return stuckDegraded ? `${noConsumerQueueCount}+` : String(noConsumerQueueCount);
  if (warningCount > 0) return stuckDegraded ? `${warningCount}+` : String(warningCount);
  return stuckDegraded ? "?" : null;
}

export function overviewStuckBadgeTone(
  criticalCount: number,
  warningCount: number,
  stuckDegraded = false,
  noConsumerQueueCount = 0,
): "critical" | "warning" | null {
  if (criticalCount > 0 || noConsumerQueueCount > 0) return "critical";
  if (warningCount > 0 || stuckDegraded) return "warning";
  return null;
}

function stuckTaskCountLabel(
  count: number,
  severity: StuckTask["severity"],
  queueCount = 0,
): string {
  if (queueCount > 0 && queueCount === count) {
    const queueLabel = count === 1 ? "stuck queue" : "stuck queues";
    return `${count} ${severity} ${queueLabel}`;
  }
  if (queueCount > 0) {
    const groupLabel = count === 1 ? "stuck group" : "stuck groups";
    return `${count} ${severity} ${groupLabel}`;
  }
  const taskLabel = count === 1 ? "stuck task" : "stuck tasks";
  return `${count} ${severity} ${taskLabel}`;
}

function noConsumerQueueCountLabel(count: number): string {
  const queueLabel = count === 1 ? "worker queue has" : "worker queues have";
  return `${count} ${queueLabel} no consumers`;
}

export function overviewStuckBadgeSrText(
  criticalCount: number,
  warningCount: number,
  stuckDegraded = false,
  noConsumerQueueCount = 0,
  criticalQueueCount = 0,
  warningQueueCount = 0,
): string {
  const prefix = stuckDegraded ? "at least " : "";
  const noConsumerSuffix = noConsumerQueueCount > 0
    ? ` and ${noConsumerQueueCountLabel(noConsumerQueueCount)}`
    : "";
  if (criticalCount > 0) {
    const warningSuffix = warningCount > 0
      ? ` and ${stuckTaskCountLabel(warningCount, "warning", warningQueueCount)}`
      : "";
    return `${prefix}${stuckTaskCountLabel(
      criticalCount,
      "critical",
      criticalQueueCount,
    )}${noConsumerSuffix}${warningSuffix}`;
  }
  if (noConsumerQueueCount > 0) {
    const warningSuffix = warningCount > 0
      ? ` and ${stuckTaskCountLabel(warningCount, "warning")}`
      : "";
    return `${prefix}${noConsumerQueueCountLabel(noConsumerQueueCount)}${warningSuffix}`;
  }
  if (warningCount > 0) {
    return `${prefix}${stuckTaskCountLabel(warningCount, "warning", warningQueueCount)}`;
  }
  return "unknown stuck task count";
}

export interface OverviewStuckCounts {
  criticalCount: number;
  warningCount: number;
  noConsumerQueueCount: number;
  criticalQueueCount: number;
  warningQueueCount: number;
}

export function overviewStuckCounts(
  stuckTasks: StuckTask[],
  noConsumerWorkerQueueCount = 0,
): OverviewStuckCounts {
  const criticalGroups = groupedStuckTasks(
    stuckTasks.filter((task) => task.severity === "critical"),
  );
  const warningGroups = groupedStuckTasks(
    stuckTasks.filter((task) => task.severity === "warning"),
  );
  const criticalQueueCount = criticalGroups.filter(stuckTaskGroupIsNoConsumerBacklog).length;
  const warningQueueCount = warningGroups.filter(stuckTaskGroupIsNoConsumerBacklog).length;
  return {
    criticalCount: criticalGroups.length,
    warningCount: warningGroups.length,
    noConsumerQueueCount: Math.max(0, noConsumerWorkerQueueCount - criticalQueueCount),
    criticalQueueCount,
    warningQueueCount,
  };
}

function isTab(value: string | null): value is Tab {
  return TABS.some((tab) => tab.id === value);
}

export function isRecentResultFilter(value: string | null): value is RecentResultFilter {
  return RECENT_RESULT_FILTERS.some((filter) => filter === value);
}

function getTabFromUrl(): Tab {
  if (typeof window === "undefined") return "overview";

  const tab = new URLSearchParams(window.location.search).get("tab");
  return isTab(tab) ? tab : "overview";
}

function getResultsFilterFromUrl(): RecentResultFilter {
  if (typeof window === "undefined") return "all";

  const params = new URLSearchParams(window.location.search);
  if (params.get("tab") !== "results") return "all";

  const filter = params.get(RESULT_FILTER_PARAM);
  return isRecentResultFilter(filter) ? filter : "all";
}

function setDashboardUrlState(tab: Tab, resultsFilter: RecentResultFilter) {
  const url = new URL(window.location.href);
  url.searchParams.delete("token");

  if (tab === "overview") {
    url.searchParams.delete("tab");
  } else {
    url.searchParams.set("tab", tab);
  }

  if (tab === "results" && resultsFilter !== "all") {
    url.searchParams.set(RESULT_FILTER_PARAM, resultsFilter);
  } else {
    url.searchParams.delete(RESULT_FILTER_PARAM);
  }

  window.history.replaceState(null, "", url);
}

export function sanitizeDashboardUrl(url: URL): boolean {
  const before = url.href;
  url.searchParams.delete("token");

  const tab = url.searchParams.get("tab");
  if (tab !== null && !isTab(tab)) {
    url.searchParams.delete("tab");
  }
  const sanitizedTab = url.searchParams.get("tab");
  const normalizedTab: Tab = isTab(sanitizedTab) ? sanitizedTab : "overview";
  const resultFilter = url.searchParams.get(RESULT_FILTER_PARAM);
  if (
    resultFilter !== null &&
    (normalizedTab !== "results" || !isRecentResultFilter(resultFilter) || resultFilter === "all")
  ) {
    url.searchParams.delete(RESULT_FILTER_PARAM);
  }

  return url.href !== before;
}

function syncDashboardAuthUrl() {
  const url = new URL(window.location.href);
  void bootstrapDashboardAuthCookie();
  if (sanitizeDashboardUrl(url)) {
    window.history.replaceState(null, "", url);
  }
}

export function dashboardLoadingMessage(
  connected: boolean,
  error: string | null | undefined,
): string {
  if (connected) return "Waiting for snapshot...";
  return error || "Connecting to server...";
}

export function redisDisconnectedDetail(
  connected: boolean,
  error: string | null | undefined,
): string {
  const detail = error?.trim();
  if (connected) {
    return "Dashboard server is reachable, but it cannot reach Redis.";
  }
  if (detail) {
    return `Dashboard connection issue: ${detail}. Last snapshot reported Redis unavailable.`;
  }
  return "Dashboard connection is disconnected. Last snapshot reported Redis unavailable.";
}

export default function App() {
  const { snapshot, stuckTasks, workers, connected, error, lastUpdate } = useSnapshot();
  const [activeTab, setActiveTab] = useState<Tab>(getTabFromUrl);
  const [resultsFilter, setResultsFilter] = useState<RecentResultFilter>(getResultsFilterFromUrl);
  const degradedSections = normalizedDegradedSectionSet(snapshot?.degraded_sections || []);
  const isDegraded = (section: string) => hasDegradedSection(degradedSections, section);
  const recentResultsDegraded = isDegraded("recent results");
  const resultsDepthDegraded = isDegraded("results depth");
  const deadLetterDepthDegraded = isDegraded("dead-letter depth");
  const resultAttentionCount = snapshot
    ? resultsTabAttentionCount(snapshot.recent_results)
    : 0;
  const resultsWindowIncomplete = snapshot
    ? snapshot.results_depth > snapshot.recent_results.length
    : false;
  const resultsAttentionUncertain = recentResultsDegraded
    || resultsWindowIncomplete
    || resultsDepthDegraded;
  const resultsBadge = snapshot
    ? resultsTabBadgeText(
      resultAttentionCount,
      recentResultsDegraded,
      resultsWindowIncomplete || resultsDepthDegraded,
    )
    : null;
  const deadLetterLoadedCount = snapshot?.dead_letter_entries.length ?? 0;
  const deadLetterBadge = snapshot
    ? deadLetterTabBadgeText(
      snapshot.dead_letter_count,
      deadLetterDepthDegraded,
      deadLetterLoadedCount,
    )
    : null;
  const deadLetterBadgeSrLabel = snapshot && deadLetterBadge
    ? deadLetterTabBadgeSrText(
      snapshot.dead_letter_count,
      deadLetterDepthDegraded,
      deadLetterLoadedCount,
    )
    : null;
  const stuckDetectionDegraded = isDegraded("stuck tasks");
  const rawNoConsumerWorkerQueueCount = snapshot
    ? consumerGroupNoConsumerBacklogCount(
      snapshot.consumer_groups,
      snapshot.queue_depths,
      queuedPreviewCountsByStream(snapshot.queued_tasks),
    )
    : 0;
  const {
    criticalCount: criticalStuckCount,
    warningCount: warningStuckCount,
    noConsumerQueueCount: noConsumerWorkerQueueCount,
    criticalQueueCount: criticalStuckQueueCount,
    warningQueueCount: warningStuckQueueCount,
  } = overviewStuckCounts(stuckTasks, rawNoConsumerWorkerQueueCount);
  const overviewBadge = snapshot
    ? overviewStuckBadgeText(
      criticalStuckCount,
      warningStuckCount,
      stuckDetectionDegraded,
      noConsumerWorkerQueueCount,
    )
    : null;
  const overviewBadgeTone = overviewStuckBadgeTone(
    criticalStuckCount,
    warningStuckCount,
    stuckDetectionDegraded,
    noConsumerWorkerQueueCount,
  );
  const overviewBadgeSrLabel = overviewBadge
    ? overviewStuckBadgeSrText(
      criticalStuckCount,
      warningStuckCount,
      stuckDetectionDegraded,
      noConsumerWorkerQueueCount,
      criticalStuckQueueCount,
      warningStuckQueueCount,
    )
    : null;

  useEffect(() => {
    const handleNavigation = () => {
      syncDashboardAuthUrl();
      setActiveTab(getTabFromUrl());
      setResultsFilter(getResultsFilterFromUrl());
    };

    window.addEventListener("popstate", handleNavigation);
    return () => window.removeEventListener("popstate", handleNavigation);
  }, []);

  useEffect(() => {
    syncDashboardAuthUrl();
  }, []);

  const selectTab = (tab: Tab, filter = resultsFilter) => {
    setActiveTab(tab);
    setDashboardUrlState(tab, filter);
  };

  const focusTab = (tab: Tab) => {
    requestAnimationFrame(() => {
      document.getElementById(dashboardTabId(tab))?.focus();
    });
  };

  const selectTabAndFocus = (tab: Tab, filter = resultsFilter) => {
    selectTab(tab, filter);
    focusTab(tab);
  };

  const openResultsFromOverview = (filter?: RecentResultFilter) => {
    const nextFilter = filter ?? (resultAttentionCount > 0 ? "failed" : "all");
    setResultsFilter(nextFilter);
    selectTabAndFocus("results", nextFilter);
  };

  const selectResultsFilter = (filter: RecentResultFilter) => {
    setResultsFilter(filter);
    setDashboardUrlState(activeTab, filter);
  };

  const handleTabKeyDown = (event: KeyboardEvent<HTMLButtonElement>, tab: Tab) => {
    const nextTab = nextDashboardTab(tab, event.key);
    if (!nextTab) return;

    event.preventDefault();
    selectTabAndFocus(nextTab);
  };

  const renderTabPanelContent = (tab: Tab) => {
    if (tab !== activeTab) return null;

    if (!snapshot) {
      return (
        <div className="flex items-center justify-center h-64">
          <div className="text-zinc-500">
            {dashboardLoadingMessage(connected, error)}
          </div>
        </div>
      );
    }

    if (!snapshot.redis_ok) {
      return (
        <div className="flex items-center justify-center h-64">
          <div
            role="alert"
            className="rounded-lg border border-red-500/30 bg-red-500/10 px-6 py-4"
          >
            <div className="text-red-400 font-medium">Redis Disconnected</div>
            <div className="text-sm text-red-300 mt-1">
              {redisDisconnectedDetail(connected, error)}
            </div>
          </div>
        </div>
      );
    }

    return (
      <>
        <SnapshotDegraded sections={snapshot.degraded_sections} />
        {tab === "overview" && (
          <>
            <StuckAlerts
              stuckTasks={stuckTasks}
              queuedTasks={snapshot.queued_tasks}
              consumerGroups={snapshot.consumer_groups}
              queueDepths={snapshot.queue_depths}
              degraded={isDegraded("stuck tasks")}
            />
            <QueueDepths
              snapshot={snapshot}
              degraded={isDegraded("queue depths")}
              resultsDepthDegraded={resultsDepthDegraded}
              deadLetterDepthDegraded={deadLetterDepthDegraded}
            />
            <ResultHealth
              results={snapshot.recent_results}
              total={snapshot.results_depth}
              degraded={recentResultsDegraded}
              depthDegraded={resultsDepthDegraded}
              onOpenResults={openResultsFromOverview}
            />
            <WorkerPool
              pools={snapshot.worker_pool}
              workers={workers}
              consumerGroups={snapshot.consumer_groups}
              queueDepths={snapshot.queue_depths}
              queuedTasks={snapshot.queued_tasks}
              poolDegraded={isDegraded("worker pool")}
              workerDiscoveryDegraded={isDegraded("worker discovery")}
            />
            <ProviderHealth
              health={snapshot.provider_health}
              degraded={isDegraded("provider health")}
            />
            <AttemptCounts
              attempts={snapshot.attempt_counts}
              maxAttempts={snapshot.dashboard_policy.max_attempts}
              degraded={isDegraded("attempt counts")}
            />
            <ActiveWork
              locks={snapshot.locks}
              stuckTasks={stuckTasks}
              degraded={isDegraded("active locks")}
            />
            <ConsumerGroups
              groups={snapshot.consumer_groups}
              queueDepths={snapshot.queue_depths}
              queuedTasks={snapshot.queued_tasks}
              degraded={isDegraded("consumer groups")}
            />
          </>
        )}

        {tab === "kanban" && (
          <Kanban snapshot={snapshot} stuckTasks={stuckTasks} />
        )}

        {tab === "results" && (
          <RecentResults
            results={snapshot.recent_results}
            total={snapshot.results_depth}
            degraded={recentResultsDegraded}
            depthDegraded={resultsDepthDegraded}
            filter={resultsFilter}
            onFilterChange={selectResultsFilter}
          />
        )}

        {tab === "dead-letters" && (
          <DeadLetters
            entries={snapshot.dead_letter_entries}
            total={snapshot.dead_letter_count}
            entriesDegraded={isDegraded("dead-letter entries")}
            depthDegraded={deadLetterDepthDegraded}
          />
        )}
      </>
    );
  };

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 flex flex-col">
      {/* Header */}
      <header className="border-b border-zinc-800 px-4 py-4 sm:px-6">
        <div className="flex min-w-0 flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <h1 className="shrink-0 text-xl font-bold tracking-tight">
            Orcest Dashboard
          </h1>
          <ConnectionStatus connected={connected} error={error} lastUpdate={lastUpdate} />
        </div>

        {/* Tabs */}
        <nav
          className="mt-4 flex gap-1 overflow-x-auto pb-1"
          role="tablist"
          aria-label="Dashboard views"
        >
          {TABS.map((tab) => (
            <button
              key={tab.id}
              id={dashboardTabId(tab.id)}
              role="tab"
              aria-selected={activeTab === tab.id}
              aria-controls={dashboardTabControls(tab.id)}
              tabIndex={activeTab === tab.id ? 0 : -1}
              className={`shrink-0 whitespace-nowrap rounded-lg px-3 py-2 text-sm transition-colors sm:px-4 ${
                activeTab === tab.id
                  ? "bg-zinc-800 text-zinc-100"
                  : "text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/50"
              }`}
              onClick={() => selectTab(tab.id)}
              onKeyDown={(event) => handleTabKeyDown(event, tab.id)}
            >
              {tab.label}
              {tab.id === "overview" && overviewBadge && overviewBadgeTone && (
                <span className={`ml-2 inline-flex items-center rounded-full px-1.5 py-0.5 text-xs ${
                  overviewBadgeTone === "critical"
                    ? "bg-red-500/20 text-red-400"
                    : "bg-yellow-500/20 text-yellow-300"
                }`}>
                  <span aria-hidden="true">{overviewBadge}</span>
                  <span className="sr-only">
                    {" "}{overviewBadgeSrLabel}
                  </span>
                </span>
              )}
              {tab.id === "results" && resultsBadge && (
                <span className={`ml-2 inline-flex items-center rounded-full px-1.5 py-0.5 text-xs ${
                  resultsAttentionUncertain && resultAttentionCount === 0
                    ? "bg-yellow-500/20 text-yellow-300"
                    : "bg-red-500/20 text-red-400"
                }`}>
                  {resultsAttentionUncertain && resultAttentionCount === 0 ? (
                    <>
                      <span aria-hidden="true">{resultsBadge}</span>
                      <span className="sr-only">
                        {resultsTabBadgeSrText(
                          resultAttentionCount,
                          recentResultsDegraded,
                          resultsWindowIncomplete || resultsDepthDegraded,
                        )}
                      </span>
                    </>
                  ) : (
                    <>
                      {resultsBadge}
                      <span className="sr-only">
                        {" "}{resultsTabBadgeSrText(
                          resultAttentionCount,
                          recentResultsDegraded,
                          resultsWindowIncomplete || resultsDepthDegraded,
                        )}
                      </span>
                    </>
                  )}
                </span>
              )}
              {tab.id === "dead-letters" && deadLetterBadge && (
                <span className={`ml-2 inline-flex items-center rounded-full px-1.5 py-0.5 text-xs ${
                  deadLetterDepthDegraded
                    ? "bg-yellow-500/20 text-yellow-300"
                    : "bg-red-500/20 text-red-400"
                }`}>
                  {deadLetterDepthDegraded ? (
                    <>
                      <span aria-hidden="true">{deadLetterBadge}</span>
                      <span className="sr-only">
                        {deadLetterBadgeSrLabel}
                      </span>
                    </>
                  ) : (
                    <>
                      {deadLetterBadge}
                      <span className="sr-only">
                        {" "}{deadLetterBadgeSrLabel}
                      </span>
                    </>
                  )}
                </span>
              )}
            </button>
          ))}
        </nav>
      </header>

      {/* Content */}
      <main className="flex min-h-0 flex-1 flex-col overflow-auto px-4 py-5 sm:px-6 sm:py-6">
        {TABS.map((tab) => {
          const active = tab.id === activeTab;
          return (
            <section
              key={tab.id}
              id={dashboardTabPanelId(tab.id)}
              role="tabpanel"
              aria-labelledby={dashboardTabId(tab.id)}
              hidden={!active}
              className={active ? dashboardTabPanelClassName(tab.id, snapshot?.redis_ok) : "hidden"}
            >
              {renderTabPanelContent(tab.id)}
            </section>
          );
        })}
      </main>
    </div>
  );
}
