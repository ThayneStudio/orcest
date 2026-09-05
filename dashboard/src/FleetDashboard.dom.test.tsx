/** @vitest-environment happy-dom */
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import FleetDashboard from "./FleetDashboard";
import type { FleetWork, WorkView } from "./lib/workTypes";
import { resetDashboardAuthTokenForTesting } from "./lib/authToken";
vi.mock("./components/TaskOutputPanel", () => ({
  TaskOutputPanel: ({
    params,
  }: {
    params: { taskId: string; historical: boolean };
  }) => (
    <div data-testid="agent-output">
      {params.taskId} {params.historical ? "historical" : "live"}
    </div>
  ),
}));
const work = (id: string, more: Partial<FleetWork> = {}): FleetWork => ({
  id,
  project: "org/repo",
  prefix: "project-a",
  kind: "issue",
  number: Number(id),
  title: `Work ${id}`,
  description: "Task context",
  url: `https://github.com/org/repo/issues/${id}`,
  stage: "upcoming",
  activity: "waiting",
  reason: "Waiting for dependency",
  next: "Resume when dependency closes",
  observedAt: Date.now() / 1000,
  startedAt: null,
  completedAt: null,
  stale: false,
  outcome: null,
  needsHuman: false,
  blockers: [],
  branch: "",
  headSha: "",
  relatedPr: null,
  latestAttempt: null,
  ...more,
});
const run = {
  taskId: "attempt-new",
  workerId: "vm-1",
  workerPrefix: "project-a",
  provider: "codex",
  model: "test",
  accountId: "account-1",
  startedAt: 123,
  finishedAt: null,
  status: "running",
  outputPrefix: "project-a",
};
const current = work("2", {
  stage: "in_progress",
  activity: "executing",
  reason: "Execution in progress",
  latestAttempt: run,
  attempts: [run, { ...run, taskId: "attempt-old", startedAt: 120 }],
});
const exception = work("3", {
  needsHuman: true,
  reason: "Repository access unavailable",
});
const data: WorkView = {
  version: 1,
  pools: [],
  fetchedAt: Date.now() / 1000,
  items: [work("1"), current, exception],
  total: 3,
  nextOffset: null,
  projects: ["org/repo", "org/other"],
  accounts: [
    {
      id: "account-1",
      provider: "codex",
      models: ["test"],
      projects: ["org/repo"],
      availability: "cooldown",
      resetsAt: null,
      observedAt: 123,
      usage: null,
    },
  ],
  workers: [],
  coverage: "complete",
  notices: [],
  counts: {
    upcoming: 2,
    in_progress: 1,
    done: 0,
    unknown: 0,
    queued: 0,
    waiting: 2,
    needsHuman: 1,
    running: 1,
  },
};
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
  resetDashboardAuthTokenForTesting();
  window.history.replaceState(null, "", "/");
});
it("scrubs link credentials immediately and waits for cookie bootstrap before polling", async () => {
  window.history.replaceState(null, "", "/?token=legacy");
  let finish!: (value: { ok: boolean }) => void;
  const bootstrap = new Promise<{ ok: boolean }>((resolve) => {
    finish = resolve;
  });
  const requests = vi.fn(async (input: string) =>
    input.includes("/api/auth/bootstrap")
      ? bootstrap
      : { ok: true, status: 200, json: async () => data },
  );
  vi.stubGlobal("fetch", requests);
  render(<FleetDashboard />);
  expect(window.location.search).toBe("");
  expect(requests).toHaveBeenCalledTimes(1);
  expect(requests.mock.calls[0][0]).toContain(
    "/api/auth/bootstrap?token=legacy",
  );
  finish({ ok: true });
  await screen.findByRole("button", { name: "Open org/repo #2: Work 2" });
  expect(
    requests.mock.calls
      .filter(([url]) => url.startsWith("/api/work"))
      .every(([url]) => !url.includes("token=")),
  ).toBe(true);
});
it("retains legacy authentication for work and detail when cookie bootstrap fails", async () => {
  window.history.replaceState(null, "", "/?token=legacy");
  const requests = vi.fn(async (input: string) =>
    input.includes("/api/auth/bootstrap")
      ? { ok: false, status: 503 }
      : {
          ok: true,
          status: 200,
          json: async () => (input.includes("/api/work/2") ? current : data),
        },
  );
  vi.stubGlobal("fetch", requests);
  render(<FleetDashboard />);
  fireEvent.click(
    await screen.findByRole("button", { name: "Open org/repo #2: Work 2" }),
  );
  await screen.findByTestId("agent-output");
  expect(window.location.search).toBe("");
  const workRequests = requests.mock.calls.filter(([url]) =>
    url.startsWith("/api/work"),
  );
  expect(workRequests.length).toBeGreaterThanOrEqual(2);
  expect(
    workRequests.every(
      ([url]) =>
        new URL(url, window.location.origin).searchParams.get("token") ===
        "legacy",
    ),
  ).toBe(true);
});
it("renders lifecycle columns, opens live and historical context, and separates account capacity from workers", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: string) => ({
      ok: true,
      status: 200,
      json: async () =>
        input.includes("/api/work/2")
          ? current
          : input.includes("attention=true")
            ? { ...data, items: [exception] }
            : data,
    })),
  );
  render(<FleetDashboard />);
  await screen.findByRole("button", { name: "Open org/repo #2: Work 2" });
  expect(
    within(screen.getByRole("region", { name: "Upcoming" })).getByText(
      "Work 1",
    ),
  ).toBeTruthy();
  expect(
    within(screen.getByRole("region", { name: "In progress" })).getByText(
      "Work 2",
    ),
  ).toBeTruthy();
  expect(screen.queryByText("Work 3")).toBeNull();
  fireEvent.click(
    screen.getByRole("button", { name: "Open org/repo #2: Work 2" }),
  );
  expect(await screen.findByTestId("agent-output")).toHaveProperty(
    "textContent",
    "attempt-new live",
  );
  fireEvent.change(screen.getByLabelText("Agent session"), {
    target: { value: "attempt-old" },
  });
  expect(screen.getByTestId("agent-output").textContent).toBe(
    "attempt-old historical",
  );
  fireEvent.click(screen.getByRole("tab", { name: "Context" }));
  expect(screen.getByText("Task context")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Close detail" }));
  fireEvent.click(screen.getByRole("button", { name: /needs you/ }));
  expect(await screen.findByText("Work 3")).toBeTruthy();
  fireEvent.click(screen.getByRole("button", { name: "Close detail" }));
  fireEvent.click(screen.getByRole("button", { name: "▤ Fleet" }));
  expect(screen.getByText("Agent accounts")).toBeTruthy();
  expect(screen.getByText("0 reporting")).toBeTruthy();
  expect(screen.getByText("cooldown")).toBeTruthy();
  fireEvent.change(screen.getByLabelText("Project"), {
    target: { value: "org/other" },
  });
  await waitFor(() =>
    expect(
      vi
        .mocked(fetch)
        .mock.calls.some(([url]) =>
          String(url).includes("project=org%2Fother"),
        ),
    ).toBe(true),
  );
});
