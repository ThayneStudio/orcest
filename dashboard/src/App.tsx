import { useEffect, useState } from "react";
import { useSnapshot } from "./hooks/useSnapshot";
import { ConnectionStatus } from "./components/ConnectionStatus";
import { StuckAlerts } from "./components/StuckAlerts";
import { QueueDepths } from "./components/QueueDepths";
import { ActiveWork } from "./components/ActiveWork";
import { ConsumerGroups } from "./components/ConsumerGroups";
import { RecentResults } from "./components/RecentResults";
import { DeadLetters } from "./components/DeadLetters";
import { Kanban } from "./components/Kanban";

type Tab = "overview" | "kanban" | "results" | "dead-letters";

const TABS: { id: Tab; label: string }[] = [
  { id: "overview", label: "Overview" },
  { id: "kanban", label: "Kanban" },
  { id: "results", label: "Results" },
  { id: "dead-letters", label: "Dead Letters" },
];

function isTab(value: string | null): value is Tab {
  return TABS.some((tab) => tab.id === value);
}

function getTabFromUrl(): Tab {
  if (typeof window === "undefined") return "overview";

  const tab = new URLSearchParams(window.location.search).get("tab");
  return isTab(tab) ? tab : "overview";
}

function setTabInUrl(tab: Tab) {
  const url = new URL(window.location.href);

  if (tab === "overview") {
    url.searchParams.delete("tab");
  } else {
    url.searchParams.set("tab", tab);
  }

  window.history.replaceState(null, "", url);
}

export default function App() {
  const { snapshot, stuckTasks, workers, connected, lastUpdate } = useSnapshot();
  const [activeTab, setActiveTab] = useState<Tab>(getTabFromUrl);

  useEffect(() => {
    const handleNavigation = () => setActiveTab(getTabFromUrl());

    window.addEventListener("popstate", handleNavigation);
    return () => window.removeEventListener("popstate", handleNavigation);
  }, []);

  const selectTab = (tab: Tab) => {
    setActiveTab(tab);
    setTabInUrl(tab);
  };

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100 flex flex-col">
      {/* Header */}
      <header className="border-b border-zinc-800 px-6 py-4">
        <div className="flex items-center justify-between">
          <h1 className="text-xl font-bold tracking-tight">
            Orcest Dashboard
          </h1>
          <ConnectionStatus connected={connected} lastUpdate={lastUpdate} />
        </div>

        {/* Tabs */}
        <nav className="flex gap-1 mt-4" role="tablist">
          {TABS.map((tab) => (
            <button
              key={tab.id}
              role="tab"
              aria-selected={activeTab === tab.id}
              className={`px-4 py-2 text-sm rounded-lg transition-colors ${
                activeTab === tab.id
                  ? "bg-zinc-800 text-zinc-100"
                  : "text-zinc-400 hover:text-zinc-200 hover:bg-zinc-800/50"
              }`}
              onClick={() => selectTab(tab.id)}
            >
              {tab.label}
              {tab.id === "dead-letters" && snapshot && snapshot.dead_letter_count > 0 && (
                <span className="ml-2 inline-flex items-center rounded-full bg-red-500/20 px-1.5 py-0.5 text-xs text-red-400">
                  {snapshot.dead_letter_count}
                </span>
              )}
            </button>
          ))}
        </nav>
      </header>

      {/* Content */}
      <main className="flex-1 px-6 py-6 overflow-auto">
        {!snapshot ? (
          <div className="flex items-center justify-center h-64">
            <div className="text-zinc-500">
              {connected ? "Loading..." : "Connecting to server..."}
            </div>
          </div>
        ) : !snapshot.redis_ok ? (
          <div className="flex items-center justify-center h-64">
            <div className="rounded-lg border border-red-500/30 bg-red-500/10 px-6 py-4">
              <div className="text-red-400 font-medium">Redis Disconnected</div>
              <div className="text-sm text-red-300 mt-1">
                Cannot reach Redis. Check connection.
              </div>
            </div>
          </div>
        ) : (
          <div className="space-y-6">
            {activeTab === "overview" && (
              <>
                <StuckAlerts stuckTasks={stuckTasks} />
                <QueueDepths snapshot={snapshot} />
                <ActiveWork locks={snapshot.locks} stuckTasks={stuckTasks} />
                <ConsumerGroups groups={snapshot.consumer_groups} />
              </>
            )}

            {activeTab === "kanban" && (
              <Kanban snapshot={snapshot} stuckTasks={stuckTasks} />
            )}

            {activeTab === "results" && (
              <RecentResults results={snapshot.recent_results} />
            )}

            {activeTab === "dead-letters" && (
              <DeadLetters
                entries={snapshot.dead_letter_entries}
                total={snapshot.dead_letter_count}
              />
            )}

          </div>
        )}
      </main>
    </div>
  );
}
