import { useEffect, useRef, useState } from "react";
import { TaskOutputPanel } from "./components/TaskOutputPanel";
import { bootstrapDashboardAuthCookie } from "./lib/authToken";
import type { FleetWork, WorkView, WorkAttempt } from "./lib/workTypes";
import "./fleet.css";

const stages = [
  { id: "upcoming", label: "Upcoming", hint: "Ready or waiting to start" },
  {
    id: "in_progress",
    label: "In progress",
    hint: "Started · including checks and retries",
  },
  { id: "done", label: "Done", hint: "Recently completed" },
] as const;
function ago(time: number | null) {
  if (time === null) return "Time unavailable";
  const mins = Math.max(0, Math.floor((Date.now() / 1000 - time) / 60));
  return mins < 1
    ? "Just now"
    : mins < 60
      ? `${mins}m ago`
      : `${Math.floor(mins / 60)}h ago`;
}
function SourceLink({ work }: { work: FleetWork }) {
  return (
    <a href={work.url} target="_blank" rel="noreferrer">
      {work.project} #{work.number} ↗
    </a>
  );
}
async function read<T>(url: string, signal?: AbortSignal): Promise<T> {
  const response = await fetch(url, { credentials: "same-origin", signal });
  if (response.status === 401) {
    window.location.assign("/sign-in");
    throw new Error("Sign-in required");
  }
  if (!response.ok)
    throw new Error(
      response.status === 404
        ? "Work is not in the current dashboard scope."
        : "The dashboard could not refresh its data.",
    );
  return response.json();
}
async function readPages(
  params: URLSearchParams,
  target: number,
  signal: AbortSignal,
): Promise<WorkView> {
  const items: FleetWork[] = [];
  let offset = 0;
  let result: WorkView;
  do {
    params.set("limit", String(Math.min(500, target - items.length)));
    params.set("offset", String(offset));
    result = await read<WorkView>(`/api/work?${params}`, signal);
    items.push(...result.items);
    offset = result.nextOffset ?? 0;
  } while (result.nextOffset !== null && items.length < target);
  return { ...result, items };
}
function WorkCard({ work, open }: { work: FleetWork; open: () => void }) {
  return (
    <button
      className={`fleet-card ${work.activity}`}
      onClick={open}
      aria-label={`Open ${work.project} #${work.number}: ${work.title}`}
    >
      <div className="fleet-meta">
        <span>{work.project.split("/").at(-1)}</span>
        <span>
          {work.kind === "pr" ? "PR" : "Issue"} #{work.number}
        </span>
      </div>
      <h3>{work.title}</h3>
      <span className="fleet-kind">
        {work.kind === "pr" ? "Pull request" : "Issue"}
      </span>
      <p className="fleet-activity">
        {work.activity === "executing" ? "⌁" : "◷"} {work.reason}
      </p>
      <div className="fleet-card-foot">
        <span>
          {work.latestAttempt
            ? `${work.activity === "executing" ? "" : "Last: "}${work.latestAttempt.provider || "Agent"}`
            : work.stage === "upcoming"
              ? "No run recorded"
              : "No agent session"}
        </span>
        <span>
          {work.stale
            ? "Stale observation"
            : ago(work.stage === "done" ? work.completedAt : work.observedAt)}
        </span>
      </div>
    </button>
  );
}
export default function FleetDashboard() {
  const [view, setView] = useState<"board" | "fleet">("board"),
    [data, setData] = useState<WorkView | null>(null),
    [error, setError] = useState(""),
    [project, setProject] = useState(""),
    [query, setQuery] = useState(""),
    [limit, setLimit] = useState(150),
    [selected, setSelected] = useState<string | null>(null),
    [attention, setAttention] = useState(false);
  const [detail, setDetail] = useState<FleetWork | null>(null),
    [detailError, setDetailError] = useState(""),
    [tab, setTab] = useState("context"),
    [attemptId, setAttemptId] = useState<string | null>(null);
  const [exceptions, setExceptions] = useState<FleetWork[]>([]),
    [exceptionError, setExceptionError] = useState("");
  const dialog = useRef<HTMLDialogElement>(null);
  const returnFocus = useRef<HTMLElement | null>(null);
  useEffect(() => {
    void bootstrapDashboardAuthCookie().then(() => {
      const url = new URL(window.location.href);
      url.searchParams.delete("token");
      window.history.replaceState(null, "", url);
    });
  }, []);
  useEffect(() => {
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        const params = new URLSearchParams({
          project,
          q: query,
          limit: String(limit),
        });
        const next = await readPages(params, limit, abort.signal);
        setData(next);
        setError("");
      } catch (e) {
        if (!abort.signal.aborted) setError((e as Error).message);
      } finally {
        if (!abort.signal.aborted) timer = setTimeout(poll, 3000);
      }
    }
    void poll();
    return () => {
      abort.abort();
      clearTimeout(timer);
    };
  }, [project, query, limit]);
  useEffect(() => {
    if (!attention) return;
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        const result = await readPages(
          new URLSearchParams({ attention: "true" }),
          5000,
          abort.signal,
        );
        setExceptions(result.items);
        setExceptionError("");
      } catch (e) {
        if (!abort.signal.aborted) setExceptionError((e as Error).message);
      } finally {
        if (!abort.signal.aborted) timer = setTimeout(poll, 3000);
      }
    }
    void poll();
    return () => {
      abort.abort();
      clearTimeout(timer);
    };
  }, [attention]);
  useEffect(() => {
    if (!selected) {
      setDetail(null);
      return;
    }
    const abort = new AbortController();
    let timer: ReturnType<typeof setTimeout>;
    async function poll() {
      try {
        setDetail(
          await read<FleetWork>(
            `/api/work/${encodeURIComponent(selected!)}`,
            abort.signal,
          ),
        );
        setDetailError("");
      } catch (e) {
        if (!abort.signal.aborted) setDetailError((e as Error).message);
      } finally {
        if (!abort.signal.aborted) timer = setTimeout(poll, 3000);
      }
    }
    void poll();
    return () => {
      abort.abort();
      clearTimeout(timer);
    };
  }, [selected]);
  useEffect(() => {
    if (selected || attention) {
      if (!dialog.current?.open) dialog.current?.showModal();
    } else if (dialog.current?.open) {
      dialog.current.close();
      returnFocus.current?.focus();
    }
  }, [selected, attention]);
  function open(work: FleetWork) {
    returnFocus.current = document.activeElement as HTMLElement;
    setSelected(work.id);
    setDetail(work);
    setDetailError("");
    setAttention(false);
    setAttemptId(null);
    setTab(work.activity === "executing" ? "output" : "context");
  }
  function close() {
    setSelected(null);
    setAttention(false);
    setDetailError("");
  }
  const allocated = data?.pools.reduce((n, p) => n + p.active_count, 0) ?? 0;
  const warm = data?.pools.reduce((n, p) => n + p.idle_count, 0) ?? 0;
  const items = data?.items || [],
    chosen = detail;
  const attempts =
    chosen?.attempts || (chosen?.latestAttempt ? [chosen.latestAttempt] : []);
  const currentAttempt =
    attempts.find((a) => a.taskId === attemptId) || attempts[0];
  function outputParams(a: WorkAttempt) {
    return {
      workerId: a.workerId,
      taskId: a.taskId,
      prefix: a.outputPrefix || null,
      historical:
        a.status !== "running" ||
        chosen?.activity !== "executing" ||
        a.taskId !== chosen.latestAttempt?.taskId,
    };
  }
  return (
    <div className="fleet-app">
      <header className="fleet-topbar">
        <strong>
          <span>▱</span> orcest
        </strong>
        <div>
          <a href="/?view=diagnostics">Diagnostics</a>
          <button
            onClick={async () => {
              try {
                const response = await fetch("/api/auth/logout", {
                  method: "POST",
                });
                if (!response.ok) throw new Error();
                window.location.assign("/sign-in");
              } catch {
                setError("Unable to sign out. Please try again.");
              }
            }}
          >
            Sign out
          </button>
        </div>
      </header>
      <main>
        <h1 className="sr-only">Orcest fleet</h1>
        <div className="fleet-summary-bar">
          {data?.pools.length ? (
            <span title="Allocated VMs out of all currently provisioned VMs; this is not the configured capacity limit.">
              <b>
                {allocated} / {allocated + warm}
              </b>{" "}
              VMs allocated
            </span>
          ) : null}
          <span>
            <b>{data?.counts.running ?? "—"}</b> active runs
          </span>
          <span>
            <b>{data?.counts.queued ?? "—"}</b> queued
          </span>
          <span>
            <b>{data?.counts.waiting ?? "—"}</b> waiting
          </span>
          <button
            className="fleet-attention"
            onClick={() => {
              returnFocus.current = document.activeElement as HTMLElement;
              setAttention(true);
              setSelected(null);
            }}
          >
            <b>{data?.counts.needsHuman ?? "—"}</b> needs you →
          </button>
        </div>
        <div className="fleet-toolbar">
          <nav aria-label="Dashboard views">
            <button
              aria-current={view === "board" ? "page" : undefined}
              onClick={() => setView("board")}
            >
              ▦ Board
            </button>
            <button
              aria-current={view === "fleet" ? "page" : undefined}
              onClick={() => setView("fleet")}
            >
              ▤ Fleet
            </button>
          </nav>
          <div className="fleet-filters">
            <label>
              <span className="sr-only">Project</span>
              <select
                value={project}
                onChange={(e) => {
                  setProject(e.target.value);
                  setLimit(150);
                }}
              >
                <option value="">All projects</option>
                {data?.projects.map((p) => (
                  <option key={p}>{p}</option>
                ))}
              </select>
            </label>
            <label>
              <span className="sr-only">Search work</span>
              <input
                type="search"
                placeholder="Search work…"
                value={query}
                onChange={(e) => {
                  setQuery(e.target.value);
                  setLimit(150);
                }}
              />
            </label>
          </div>
        </div>
        {error && (
          <div className="fleet-warning" role="alert">
            {error} {data ? "Showing the last successful snapshot." : ""}
          </div>
        )}
        {data?.notices.map((n) => (
          <p className="fleet-warning" key={n}>
            {n}
          </p>
        ))}
        {!data && !error && (
          <div className="fleet-empty" role="status">
            Loading fleet observations…
          </div>
        )}
        {view === "board" && data && (
          <>
            <div className="fleet-board">
              {stages.map((stage) => {
                const column = items.filter(
                  (w) => w.stage === stage.id && !w.needsHuman,
                );
                return (
                  <section
                    className={stage.id}
                    key={stage.id}
                    aria-label={stage.label}
                  >
                    <h2>
                      {stage.label}{" "}
                      <span>
                        {column.length}
                        {data.nextOffset !== null ? "+" : ""}
                      </span>
                    </h2>
                    <p className="fleet-column-hint">{stage.hint}</p>
                    <div className="fleet-cards">
                      {column.map((w) => (
                        <WorkCard key={w.id} work={w} open={() => open(w)} />
                      ))}
                      {!column.length && (
                        <div className="fleet-empty">
                          {data.coverage === "unavailable"
                            ? "Waiting for observations"
                            : query || project
                              ? "No matching work"
                              : "Nothing here right now"}
                        </div>
                      )}
                    </div>
                  </section>
                );
              })}
            </div>
            {items.some((w) => w.stage === "unknown") && (
              <section className="fleet-unknown">
                <h2>Lifecycle unavailable</h2>
                <div className="fleet-account-grid">
                  {items
                    .filter((w) => w.stage === "unknown")
                    .map((w) => (
                      <WorkCard key={w.id} work={w} open={() => open(w)} />
                    ))}
                </div>
              </section>
            )}
            {data.nextOffset !== null && (
              <button
                className="fleet-more"
                onClick={() => setLimit((n) => Math.min(5000, n + 150))}
              >{`Load more · ${items.length} of ${data.total}`}</button>
            )}
          </>
        )}
        {view === "fleet" && data && (
          <div className="fleet-capacity">
            <section>
              <h2>
                Agent accounts <span>{data.accounts.length}</span>
              </h2>
              <p className="fleet-column-hint">
                Configured provider access, separate from worker VMs.
              </p>
              <div className="fleet-account-grid">
                {data.accounts
                  .filter((a) => !project || a.projects.includes(project))
                  .map((a) => (
                    <article className="fleet-account" key={a.id}>
                      <div className="fleet-meta">
                        <h3>{a.provider}</h3>
                        <span
                          className={
                            a.availability === "cooldown"
                              ? "fleet-attention"
                              : ""
                          }
                        >
                          {a.availability}
                        </span>
                      </div>
                      <p>{a.models.join(" / ") || "Configured model"}</p>
                      <p className="fleet-account-id">
                        Account {a.id.slice(-8)}
                      </p>
                      <dl>
                        {a.usage ? (
                          a.usage.windows.map((w) => (
                            <div key={w.name}>
                              <dt>
                                {w.name === "five_hour"
                                  ? "Five-hour"
                                  : "Weekly"}{" "}
                                remaining
                              </dt>
                              <dd>
                                {Math.round(100 - w.usedPercent)}%{" "}
                                <small>at last probe</small>
                              </dd>
                              <dt>Window reset</dt>
                              <dd>
                                {w.resetsAt
                                  ? new Date(w.resetsAt).toLocaleString()
                                  : "Not reported"}
                              </dd>
                            </div>
                          ))
                        ) : (
                          <>
                            <dt>Remaining usage</dt>
                            <dd>Not reported</dd>
                          </>
                        )}
                        <dt>Reset</dt>
                        <dd>
                          {a.resetsAt
                            ? new Date(a.resetsAt * 1000).toLocaleString()
                            : "Not reported"}
                        </dd>
                      </dl>
                      <p>{a.projects.join(", ")}</p>
                      <small>
                        {a.usage
                          ? `Usage probed ${ago(a.usage.observedAt)} · `
                          : ""}
                        Account observed {ago(a.observedAt)}
                      </small>
                    </article>
                  ))}
              </div>
              {!data.accounts.length && (
                <div className="fleet-empty">
                  No account observations available.
                </div>
              )}
            </section>
            <section>
              <h2>VM pools</h2>
              <p className="fleet-column-hint">
                Provisioned capacity reported by the pool manager. Allocated VMs
                may be starting, executing, or finishing work.
              </p>
              {data.pools.length ? (
                data.pools.map((pool) => (
                  <article className="fleet-account" key={pool.prefix}>
                    <h3>{pool.prefix || "Default pool"}</h3>
                    <p>
                      {pool.active_count} allocated · {pool.idle_count} warm
                    </p>
                    <p>
                      Allocated VMs:{" "}
                      {pool.active.map((vm) => vm.vmid).join(", ") ||
                        "None reported"}
                    </p>
                    <p>Warm VMs: {pool.idle.join(", ") || "None reported"}</p>
                  </article>
                ))
              ) : (
                <div className="fleet-empty">
                  No VM pool observations in this scope.
                </div>
              )}
            </section>
            <section>
              <h2>
                Workers <span>{data.workers.length} reporting</span>
              </h2>
              <p className="fleet-column-hint">
                Ephemeral worker processes with a current liveness signal.
                Liveness does not establish agent progress.
              </p>
              <div className="fleet-worker-list">
                {data.workers.map((w) => (
                  <article key={`${w.prefix}:${w.id}`}>
                    <strong>{w.id}</strong>
                    <span>{w.backend}</span>
                    <span>
                      {w.workId ? "Executing" : "No active task observed"}
                    </span>
                    {w.workId ? (
                      <button
                        onClick={() => {
                          const work = items.find((i) => i.id === w.workId);
                          if (work) open(work);
                          else {
                            setSelected(w.workId);
                            setDetail(null);
                          }
                        }}
                      >
                        View work →
                      </button>
                    ) : (
                      <span>Heartbeat current</span>
                    )}
                  </article>
                ))}
              </div>
              {!data.workers.length && (
                <div className="fleet-empty">
                  No current worker heartbeats in this scope.
                </div>
              )}
            </section>
          </div>
        )}
      </main>
      <footer className="fleet-footer">
        <span>
          {data
            ? `Last refresh ${ago(data.fetchedAt)}`
            : "Connecting to Orcest"}
        </span>
        <span>
          {data?.coverage === "complete"
            ? "Live observations"
            : "Coverage may be incomplete"}
        </span>
      </footer>
      <dialog
        ref={dialog}
        className="fleet-dialog"
        onCancel={close}
        onClick={(e) => {
          if (e.target === dialog.current) close();
        }}
      >
        <div className="fleet-detail">
          <button
            className="fleet-close"
            onClick={close}
            aria-label="Close detail"
          >
            ×
          </button>
          {attention ? (
            <>
              <h2>Needs you</h2>
              <p>
                Exceptional blockers that require an action outside the fleet.
              </p>
              {exceptions.map((w) => (
                <WorkCard key={w.id} work={w} open={() => open(w)} />
              ))}
              {exceptionError && (
                <p role="alert" className="fleet-warning">
                  {exceptionError}
                </p>
              )}
              {!exceptions.length && (
                <div className="fleet-empty">
                  {data?.counts.needsHuman
                    ? "Loading fleet exceptions…"
                    : "No intervention currently reported."}
                </div>
              )}
            </>
          ) : chosen ? (
            <>
              <SourceLink work={chosen} />
              <h2>{chosen.title}</h2>
              <p className="fleet-description">
                {chosen.description ||
                  "Open the source item for its full description."}
              </p>
              <div className="fleet-detail-meta">
                <span>
                  {stages.find((s) => s.id === chosen.stage)?.label ||
                    "Lifecycle unknown"}
                </span>
                <span>{chosen.reason}</span>
                {chosen.stale && (
                  <span className="fleet-attention">Stale observation</span>
                )}
              </div>
              <div className="fleet-next">
                <h3>What happens next</h3>
                <p>{chosen.next}</p>
                {chosen.blockers.map((b) => (
                  <div key={b.label}>
                    {b.workId ? (
                      <button
                        onClick={() => {
                          setSelected(b.workId);
                          setDetail(null);
                          setTab("context");
                        }}
                      >
                        {b.label} →
                      </button>
                    ) : b.url ? (
                      <a href={b.url} target="_blank" rel="noreferrer">
                        {b.label} ↗
                      </a>
                    ) : (
                      <span>{b.label}</span>
                    )}
                  </div>
                ))}
              </div>
              <div
                className="fleet-detail-tabs"
                role="tablist"
                aria-label="Work detail"
              >
                {["context", "output", "timeline"].map((name) => (
                  <button
                    role="tab"
                    id={`detail-tab-${name}`}
                    aria-controls={`detail-panel-${name}`}
                    aria-selected={tab === name}
                    tabIndex={tab === name ? 0 : -1}
                    key={name}
                    onClick={() => setTab(name)}
                    onKeyDown={(e) => {
                      const names = ["context", "output", "timeline"];
                      if (["ArrowRight", "ArrowLeft"].includes(e.key)) {
                        e.preventDefault();
                        const next =
                          names[
                            (names.indexOf(name) +
                              (e.key === "ArrowRight" ? 1 : 2)) %
                              3
                          ];
                        setTab(next);
                        document.getElementById(`detail-tab-${next}`)?.focus();
                      }
                    }}
                  >
                    {name[0].toUpperCase() + name.slice(1)}
                  </button>
                ))}
              </div>
              <div
                role="tabpanel"
                id={`detail-panel-${tab}`}
                aria-labelledby={`detail-tab-${tab}`}
              >
                {tab === "context" && (
                  <dl className="fleet-context">
                    <dt>Project</dt>
                    <dd>{chosen.project}</dd>
                    {chosen.publicationUrl && (
                      <>
                        <dt>Pull request</dt>
                        <dd>
                          <a
                            href={chosen.publicationUrl}
                            target="_blank"
                            rel="noreferrer"
                          >
                            Open published change ↗
                          </a>
                        </dd>
                      </>
                    )}
                    <dt>Branch</dt>
                    <dd>{chosen.branch || "Not reported"}</dd>
                    <dt>Current revision</dt>
                    <dd>{chosen.headSha.slice(0, 12) || "Not reported"}</dd>
                    <dt>First execution</dt>
                    <dd>
                      {chosen.startedAt
                        ? new Date(chosen.startedAt * 1000).toLocaleString()
                        : "No start recorded"}
                    </dd>
                    <dt>Last observation</dt>
                    <dd>{ago(chosen.observedAt)}</dd>
                    <dt>Outcome</dt>
                    <dd>{chosen.outcome || "Not complete"}</dd>
                    <dt>Previous provider</dt>
                    <dd>
                      {chosen.latestAttempt?.provider || "No session recorded"}
                    </dd>
                  </dl>
                )}
                {tab === "output" &&
                  (currentAttempt ? (
                    <>
                      <label className="fleet-attempt-picker">
                        Agent session
                        <select
                          value={currentAttempt.taskId}
                          onChange={(e) => setAttemptId(e.target.value)}
                        >
                          {attempts.map((a) => (
                            <option value={a.taskId} key={a.taskId}>
                              {a.provider} · {ago(a.startedAt)} · {a.status}
                            </option>
                          ))}
                        </select>
                      </label>
                      <TaskOutputPanel
                        key={currentAttempt.taskId}
                        id="fleet-agent-output"
                        params={outputParams(currentAttempt)}
                        label={`${chosen.project} #${chosen.number}`}
                        onClose={() => setTab("context")}
                        className="fleet-output"
                        autoFocusLog={false}
                      />
                    </>
                  ) : (
                    <div className="fleet-empty">
                      No agent session recorded. Output appears when an
                      execution starts.
                    </div>
                  ))}
                {tab === "timeline" && (
                  <div className="fleet-timeline">
                    {attempts.length ? (
                      attempts.map((a) => (
                        <article key={a.taskId}>
                          <h3>
                            {a.provider || "Agent"} · {a.status}
                          </h3>
                          <p>{a.workerId}</p>
                          <small>
                            Started {ago(a.startedAt)}
                            {a.finishedAt
                              ? ` · ended ${ago(a.finishedAt)}`
                              : ""}
                          </small>
                        </article>
                      ))
                    ) : (
                      <p>No attempt history recorded.</p>
                    )}
                    <article>
                      <h3>{chosen.reason}</h3>
                      <p>{chosen.next}</p>
                      <small>Observed {ago(chosen.observedAt)}</small>
                    </article>
                  </div>
                )}
              </div>
            </>
          ) : (
            <div className="fleet-empty">Loading work detail…</div>
          )}
          {detailError && (
            <div className="fleet-warning" role="alert">
              {detailError}
            </div>
          )}
        </div>
      </dialog>
    </div>
  );
}
