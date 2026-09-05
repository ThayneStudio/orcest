import type { Express } from "express";
import { redis, scanKeysMany, dashboardRedisKeyPatterns } from "./redis.js";
import type { DashboardMessage } from "./types.js";
import type {
  FleetWork,
  WorkAttempt,
  WorkView,
  FleetAccount,
  FleetWorker,
} from "../src/lib/workTypes.js";

type Fields = Record<string, string>;
function usage(raw: unknown): FleetAccount["usage"] {
  if (!raw || typeof raw !== "object") return null;
  const q = raw as Record<string, unknown>;
  const observedAt = numeric(q.observed_at);
  if (!observedAt || !Array.isArray(q.windows)) return null;
  const windows = q.windows.flatMap((v) => {
    if (!v || typeof v !== "object") return [];
    const w = v as Record<string, unknown>,
      used = numeric(w.used_percent);
    return used !== null &&
      used <= 100 &&
      ["five_hour", "seven_day"].includes(String(w.name))
      ? [
          {
            name: String(w.name),
            usedPercent: used,
            resetsAt: typeof w.resets_at === "string" ? w.resets_at : null,
          },
        ]
      : [];
  });
  return windows.length ? { observedAt, windows } : null;
}

const numeric = (v: unknown): number | null => {
  if (v === undefined || v === null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) && n >= 0 ? n : null;
};
const fq = (prefix: string, key: string) => (prefix ? `${prefix}:${key}` : key);
export const workId = (
  prefix: string,
  repo: string,
  kind: string,
  number: number,
) =>
  Buffer.from(JSON.stringify([prefix, repo, kind, number])).toString(
    "base64url",
  );
function parseArray(raw: string | undefined): unknown[] {
  try {
    const v = JSON.parse(raw || "[]");
    return Array.isArray(v) ? v : [];
  } catch {
    return [];
  }
}
function attempt(fields: Fields): WorkAttempt | null {
  if (!fields.task_id || !fields.worker_id) return null;
  return {
    taskId: fields.task_id,
    workerId: fields.worker_id,
    workerPrefix: fields.worker_prefix ?? fields.output_prefix ?? "",
    provider: fields.provider || "",
    model: fields.model || "",
    accountId: fields.account_id || "",
    startedAt: numeric(fields.started_at),
    finishedAt: numeric(fields.finished_at),
    status: fields.status || "unknown",
    outputPrefix: fields.output_prefix || "",
  };
}
const reasons: Record<string, [string, string, string]> = {
  skip_dependency: [
    "waiting",
    "Waiting for dependency",
    "Recheck eligibility when the prerequisite closes.",
  ],
  skip_queued: [
    "queued",
    "Queued · awaiting capacity",
    "A compatible worker can claim the queued task.",
  ],
  skip_locked: [
    "executing",
    "Execution in progress",
    "Observe the current attempt.",
  ],
  skip_active: [
    "waiting",
    "Attempt pending",
    "Reconcile the pending attempt and its worker.",
  ],
  skip_pending: [
    "waiting",
    "Waiting for CI",
    "Evaluate the checks when they finish.",
  ],
  skip_green: [
    "monitoring",
    "Monitoring PR",
    "Recheck review and merge eligibility.",
  ],
  skip_usage_cooldown: [
    "waiting",
    "Provider cooldown",
    "Recheck availability after the usage cooldown.",
  ],
  skip_delivery_cooldown: [
    "waiting",
    "Delivery retry cooldown",
    "Retry after the delivery cooldown clears.",
  ],
  skip_verifying: [
    "waiting",
    "Verifying handoff",
    "Confirm the published outcome before continuing.",
  ],
  skip_backoff: [
    "waiting",
    "Scheduled retry",
    "Retry after the current backoff.",
  ],
  skip_labeled: [
    "waiting",
    "Needs your attention",
    "Inspect the source item for the current blocker.",
  ],
  skip_max_attempts: [
    "waiting",
    "Retry budget reached",
    "Wait for the issue or applicable retry policy to change.",
  ],
  skip_max_total_attempts: [
    "waiting",
    "Retry budget reached",
    "Inspect the retry history and source state.",
  ],
  skip_no_checks: [
    "monitoring",
    "No checks reported",
    "Observe checks and project merge requirements.",
  ],
  skip_draft: ["waiting", "Draft PR", "Wait until the pull request is ready."],
  skip_v1_owned: [
    "monitoring",
    "Managed by workflow v1",
    "Continue under the owning workflow.",
  ],
  skip_v1_lookup_unavailable: [
    "waiting",
    "Workflow state unavailable",
    "Restore the workflow observation connection.",
  ],
  not_in_discovery: [
    "monitoring",
    "Outside current intake",
    "Observe the source item; no task is currently requested.",
  ],
  discovery_gap: [
    "monitoring",
    "Outside the latest discovery window",
    "Recheck discovery coverage and current task state.",
  ],
};
export function projectWork(
  f: Fields,
  last: WorkAttempt | null,
  message: DashboardMessage,
  now = Date.now() / 1000,
): FleetWork | null {
  const n = numeric(f.number);
  if (!n || !f.repo || !["issue", "pr"].includes(f.kind)) return null;
  const prefix = f.prefix || "",
    id = workId(prefix, f.repo, f.kind, n);
  const related =
    f.related_pr && /^\d+$/.test(f.related_pr)
      ? workId(prefix, f.repo, "pr", Number(f.related_pr))
      : null;
  const started = numeric(f.started_at),
    observed = numeric(f.observed_at);
  const current = message.snapshot.locks.find(
    (l) =>
      l.prefix === (prefix || null) &&
      l.repo === f.repo &&
      l.resource_type === f.kind &&
      l.resource_id === String(n),
  );
  const inQueue = message.snapshot.queued_tasks.some(
    (q) =>
      (q.prefix || "") === prefix &&
      q.repo === f.repo &&
      q.resource_type === f.kind &&
      q.resource_id === String(n),
  );
  let [activity, reason, next] = reasons[f.action] || [
    "ready",
    "Ready for scheduling",
    "The orchestrator will evaluate available execution capacity.",
  ];
  const outcome = f.outcome || null;
  if (f.discovery_missing === "1") {
    [activity, reason, next] = reasons.discovery_gap;
  }
  if (inQueue) {
    [activity, reason, next] = reasons.skip_queued;
  }
  if (
    current &&
    last &&
    current.task_id === last.taskId &&
    last.status === "running"
  ) {
    [activity, reason, next] = reasons.skip_locked;
  } else if (activity === "executing") {
    activity = "waiting";
    reason = "Reconciling execution";
    next = "Check the latest task and worker observations.";
  }
  if (last?.status === "running" && !current && !inQueue) {
    activity = "waiting";
    reason = "Worker status unavailable";
    next = "Reconcile the last attempt; no live execution is confirmed.";
  }
  if (outcome) {
    activity = "terminal";
    reason =
      outcome === "merged" ? "Merged" : "Closed without confirmed delivery";
    next = "No active execution recorded.";
  }
  const stage =
    outcome === "merged"
      ? "done"
      : started || f.kind === "pr"
        ? "in_progress"
        : "upcoming";
  const blockers = parseArray(f.blockers)
    .filter((x): x is string => typeof x === "string")
    .map((label) => {
      const match = label.match(/^(?:([^#\s]+)\s*)?#(\d+)$/);
      const repo = match?.[1] || f.repo,
        number = Number(match?.[2]);
      return {
        label,
        url: match ? `https://github.com/${repo}/issues/${number}` : "",
        workId:
          match && repo === f.repo
            ? workId(prefix, repo, "issue", number)
            : null,
      };
    });
  const needsHuman = f.needs_human === "1" || f.worker_needs_human === "1";
  return {
    id,
    project: f.repo,
    prefix,
    kind: f.kind as "issue" | "pr",
    number: n,
    title: f.title || `${f.kind === "pr" ? "PR" : "Issue"} #${n}`,
    description: f.description || "",
    url: `https://github.com/${f.repo}/${f.kind === "pr" ? "pull" : "issues"}/${n}`,
    stage: f.action === "skip_v1_lookup_unavailable" ? "unknown" : stage,
    activity,
    reason,
    next: needsHuman && f.human_reason ? f.human_reason : next,
    observedAt: observed,
    startedAt: started,
    completedAt: numeric(f.completed_at),
    // A verified merge is durable delivery evidence, not a live source poll.
    stale: outcome !== "merged" && (!observed || now - observed > 180),
    outcome,
    needsHuman,
    blockers,
    branch: f.branch || "",
    headSha: f.head_sha || "",
    relatedPr: related,
    publicationUrl: related
      ? `https://github.com/${f.repo}/pull/${f.related_pr}`
      : null,
    latestAttempt: last,
  };
}
async function readHashes(keys: string[]): Promise<Fields[]> {
  if (!keys.length) return [];
  const pipeline = redis.pipeline();
  keys.forEach((k) => pipeline.hgetall(k));
  const rows = await pipeline.exec();
  return keys.map((_, i) => {
    const row = rows?.[i];
    if (!row || row[0]) throw new Error("Work observation read failed");
    return row[1] as Fields;
  });
}
export async function fetchWorkView(
  message: DashboardMessage,
): Promise<WorkView> {
  if (!message.snapshot.redis_ok) throw new Error("Redis unavailable");
  const now = Date.now() / 1000;
  const [allKeys, projectKeys, heartbeatKeys] = await Promise.all([
    scanKeysMany(dashboardRedisKeyPatterns(["dashboard:work:*"])),
    scanKeysMany(dashboardRedisKeyPatterns(["dashboard:project"])),
    scanKeysMany(dashboardRedisKeyPatterns(["workers:heartbeat:*"])),
  ]);
  const keys = allKeys
    .filter((k) => /dashboard:work:(issue|pr):[^:]+:\d+$/.test(k))
    .slice(0, 5000);
  const [stored, projects] = await Promise.all([
    readHashes(keys),
    readHashes(projectKeys),
  ]);
  // Physical keys establish scope. Hash fields cannot redirect follow-up reads.
  const fields = stored.map((f, i) => {
    const m = keys[i].match(
      /^(?:(.*):)?dashboard:work:(issue|pr):([^:]+):(\d+)$/,
    )!;
    return { ...f, prefix: m[1] || "", kind: m[2], repo: m[3], number: m[4] };
  });
  const latestPipe = redis.pipeline();
  keys.forEach((k) => latestPipe.zrevrange(`${k}:attempts`, 0, 0));
  const latestIds = keys.length ? await latestPipe.exec() : [];
  const latestKeys = fields.map((f, i) => {
    const row = latestIds?.[i];
    if (row?.[0]) throw new Error("Attempt index unavailable");
    const ids = row?.[1] as string[] | undefined;
    return ids?.[0] ? fq(f.prefix || "", `dashboard:attempt:${ids[0]}`) : null;
  });
  const latest = await readHashes(latestKeys.filter((k): k is string => !!k));
  let at = 0;
  const items = fields
    .map((f, i) =>
      projectWork(
        f,
        latestKeys[i] ? attempt(latest[at++]) : null,
        message,
        now,
      ),
    )
    .filter((w): w is FleetWork => !!w);
  // Only explicit verified issue-to-PR links are consolidated. Ambiguous shared
  // publications stay as separate cards so one issue cannot hide another's work.
  const linkedCounts = new Map<string, number>();
  items.forEach((w) => {
    if (w.relatedPr)
      linkedCounts.set(w.relatedPr, (linkedCounts.get(w.relatedPr) || 0) + 1);
  });
  const byId = new Map(items.map((w) => [w.id, w]));
  const hidden = new Set<string>();
  for (const item of items) {
    const child = item.relatedPr ? byId.get(item.relatedPr) : null;
    if (child && linkedCounts.get(child.id) === 1) {
      hidden.add(child.id);
      item.stage = child.stage;
      item.activity = child.activity;
      item.reason = child.reason;
      item.next = child.needsHuman || !item.needsHuman ? child.next : item.next;
      item.branch = child.branch || item.branch;
      item.headSha = child.headSha || item.headSha;
      item.outcome = child.outcome;
      item.completedAt = child.completedAt;
      item.stale =
        child.outcome === "merged" ? false : item.stale || child.stale;
      item.needsHuman =
        child.outcome === "merged"
          ? false
          : item.needsHuman || child.needsHuman;
      if (child.latestAttempt) item.latestAttempt = child.latestAttempt;
    }
  }
  const accountMap = new Map<string, FleetAccount>();
  for (const p of projects)
    for (const raw of parseArray(p.accounts)) {
      if (!raw || typeof raw !== "object") continue;
      const a = raw as Record<string, unknown>;
      if (typeof a.id !== "string" || typeof a.provider !== "string") continue;
      const old = accountMap.get(a.id);
      const observed = numeric(p.observed_at);
      const expired = !observed || now - observed > 180;
      const availability = expired
        ? "unknown"
        : a.availability === "cooldown"
          ? "cooldown"
          : "available";
      const merged: FleetAccount = {
        usage: usage(a.quota),
        id: a.id,
        provider: a.provider,
        models: Array.isArray(a.models)
          ? a.models.filter((v): v is string => typeof v === "string")
          : [],
        projects: [p.repo],
        availability,
        resetsAt: numeric(a.resets_at),
        observedAt: observed,
      };
      if (old) {
        if ((old.usage?.observedAt || 0) > (merged.usage?.observedAt || 0))
          merged.usage = old.usage;
        merged.projects = [...new Set([...old.projects, p.repo])];
        merged.models = [...new Set([...old.models, ...merged.models])];
        if (old.availability === "cooldown") {
          merged.availability = "cooldown";
          merged.resetsAt = Math.max(old.resetsAt || 0, merged.resetsAt || 0);
        }
      }
      accountMap.set(a.id, merged);
    }
  const hp = redis.pipeline();
  heartbeatKeys.forEach((k) => {
    hp.get(k);
    hp.ttl(k);
  });
  const heartbeats = heartbeatKeys.length ? await hp.exec() : [];
  const workers: FleetWorker[] = [];
  heartbeatKeys.forEach((key, i) => {
    const raw = heartbeats?.[i * 2],
      ttl = heartbeats?.[i * 2 + 1];
    if (raw?.[0] || ttl?.[0]) throw new Error("Worker observation unavailable");
    if (!raw?.[1]) return;
    try {
      const value = JSON.parse(String(raw[1]));
      const split = key.lastIndexOf("workers:heartbeat:");
      const id = key.slice(split + 18),
        prefix = key.slice(0, Math.max(0, split - 1));
      const matches = items.filter(
        (w) =>
          w.activity === "executing" &&
          w.latestAttempt?.workerId === id &&
          w.latestAttempt.workerPrefix === prefix,
      );
      const active = matches.length === 1 ? matches[0] : null;
      if (Number(ttl?.[1]) <= 0) return;
      workers.push({
        id,
        prefix,
        backend: typeof value.backend === "string" ? value.backend : "unknown",
        revision: typeof value.revision === "string" ? value.revision : "",
        ttl: Number(ttl?.[1] || 0),
        workId: active?.id || null,
      });
    } catch {
      /* malformed worker is not counted as healthy */
    }
  });
  items.sort(
    (a, b) =>
      (b.startedAt || b.observedAt || 0) - (a.startedAt || a.observedAt || 0) ||
      a.id.localeCompare(b.id),
  );
  const visible = items.filter(
    (w) => !hidden.has(w.id) && (!w.outcome || w.outcome === "merged"),
  );
  const visibleIds = new Set(visible.map((item) => item.id));
  for (const item of visible)
    for (const blocker of item.blockers) {
      if (blocker.workId && !visibleIds.has(blocker.workId))
        blocker.workId = null;
    }
  const notices: string[] = [];
  if (!projects.length)
    notices.push(
      "Work observations are not available yet. Update the orchestrator to enable the fleet feed.",
    );
  if (
    visible.some((w) => w.stale) ||
    projects.some(
      (p) => !numeric(p.observed_at) || now - Number(p.observed_at) > 180,
    )
  )
    notices.push(
      "Some source observations are stale. Last-known work is retained.",
    );
  if (allKeys.filter((k) => /\d+$/.test(k)).length > keys.length)
    notices.push("Work inventory is limited to 5,000 records.");
  if (
    items.some(
      (w) =>
        w.activity === "monitoring" && w.reason === "Managed by workflow v1",
    )
  )
    notices.push("Workflow v1 details are not yet available in this feed.");
  if (message.snapshot.degraded_sections.length)
    notices.push("Some operational data is unavailable.");
  return {
    pools: message.snapshot.worker_pool,
    version: 1,
    fetchedAt: now,
    items: visible,
    total: visible.length,
    nextOffset: null,
    projects: [...new Set(projects.map((p) => p.repo).filter(Boolean))].sort(),
    accounts: [...accountMap.values()],
    workers,
    coverage: !projects.length
      ? "unavailable"
      : notices.length
        ? "partial"
        : "complete",
    notices,
    counts: {
      upcoming: visible.filter((w) => w.stage === "upcoming").length,
      in_progress: visible.filter((w) => w.stage === "in_progress").length,
      done: visible.filter((w) => w.stage === "done").length,
      unknown: visible.filter((w) => w.stage === "unknown").length,
      queued: visible.filter((w) => w.activity === "queued").length,
      waiting: visible.filter((w) => w.activity === "waiting").length,
      needsHuman: visible.filter((w) => w.needsHuman).length,
      running: visible.filter((w) => w.activity === "executing").length,
    },
  };
}
export function installWorkRoutes(
  app: Express,
  load: () => Promise<WorkView>,
): void {
  app.get("/api/work", async (req, res) => {
    try {
      const view = await load();
      const project = String(req.query.project || ""),
        query = String(req.query.q || "").toLowerCase();
      const offset = Number(req.query.offset || 0),
        limit = Number(req.query.limit || 150);
      if (
        !Number.isInteger(offset) ||
        offset < 0 ||
        !Number.isInteger(limit) ||
        limit < 1 ||
        limit > 500
      ) {
        res.status(400).json({ error: "Invalid page" });
        return;
      }
      const items = view.items.filter(
        (w) =>
          (req.query.attention !== "true" || w.needsHuman) &&
          (!project || w.project === project) &&
          `${w.title} ${w.number} ${w.project} ${w.reason}`
            .toLowerCase()
            .includes(query),
      );
      res.set("Cache-Control", "no-store").json({
        ...view,
        items: items.slice(offset, offset + limit),
        total: items.length,
        nextOffset: offset + limit < items.length ? offset + limit : null,
      });
    } catch {
      res.status(503).json({
        error:
          "Work observations are unavailable. Last-known work may be stale.",
      });
    }
  });
  app.get("/api/work/:id", async (req, res) => {
    try {
      const view = await load();
      const work = view.items.find((w) => w.id === req.params.id);
      if (!work) {
        res
          .status(404)
          .json({ error: "Work not found in this dashboard scope" });
        return;
      }
      const sources = [
        work,
        ...(work.relatedPr
          ? [
              {
                ...work,
                kind: "pr",
                number: Number(
                  JSON.parse(
                    Buffer.from(work.relatedPr, "base64url").toString(),
                  )[3],
                ),
              },
            ]
          : []),
      ];
      const attempts: WorkAttempt[] = [];
      for (const source of sources) {
        const ids = await redis.zrevrange(
          fq(
            source.prefix,
            `dashboard:work:${source.kind}:${source.project}:${source.number}:attempts`,
          ),
          0,
          49,
        );
        const rows = await readHashes(
          ids.map((id) => fq(source.prefix, `dashboard:attempt:${id}`)),
        );
        attempts.push(
          ...rows.map(attempt).filter((a): a is WorkAttempt => !!a),
        );
      }
      attempts.sort((a, b) => (b.startedAt || 0) - (a.startedAt || 0));
      res.set("Cache-Control", "no-store").json({ ...work, attempts });
    } catch {
      res.status(503).json({ error: "Work detail unavailable" });
    }
  });
}
