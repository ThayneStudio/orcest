import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { createInterface } from "node:readline";
import { WebSocket } from "ws";
import { createServer } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
const probe = createServer();
probe.listen(0, "127.0.0.1");
await once(probe, "listening");
const redisPort = probe.address().port;
await new Promise((resolve) => probe.close(resolve));
const redisDir = await mkdtemp(join(tmpdir(), "orcest-fleet-e2e-"));
const redisProcess = spawn(
  process.env.ORCEST_TEST_REDIS_SERVER || "redis-server",
  [
    "--bind",
    "127.0.0.1",
    "--port",
    String(redisPort),
    "--save",
    "",
    "--appendonly",
    "no",
  ],
  { cwd: redisDir, stdio: "ignore" },
);
redisProcess.on("error", (error) => {
  console.error("Redis could not start:", error.message);
  process.exit(1);
});
const fixture = spawn(
  process.env.ORCEST_TEST_PYTHON || "../.venv/bin/python",
  ["scripts/fleet-fixture.py"],
  {
    stdio: ["pipe", "pipe", "inherit"],
    env: { ...process.env, REDIS_PORT: String(redisPort) },
  },
);
const lines = createInterface({ input: fixture.stdout });
const first = await Promise.race([
  once(lines, "line"),
  once(fixture, "exit").then(([code]) => {
    throw new Error(`Fixture exited: ${code}`);
  }),
]);
const { port, taskId } = JSON.parse(first[0]);
process.env.REDIS_HOST = "127.0.0.1";
process.env.REDIS_PORT = String(port);
process.env.DASHBOARD_REDIS_PREFIXES = "fleet-e2e,fleet-shared";
process.env.DASHBOARD_TOKEN = "e2e-sign-in";
const { createDashboardServer } = await import("../build/server/index.js");
const instance = createDashboardServer({ port: 0 });
let socket;
try {
  instance.server.listen(0, "127.0.0.1");
  await once(instance.server, "listening");
  const base = `http://127.0.0.1:${instance.server.address().port}`;
  assert.equal((await fetch(base + "/api/work")).status, 401);
  const login = await fetch(base + "/api/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ token: "e2e-sign-in" }),
  });
  assert.equal(login.status, 200);
  const cookie = login.headers.get("set-cookie").split(";")[0];
  const read = async (path) => {
    const r = await fetch(base + path, { headers: { Cookie: cookie } });
    assert.equal(r.status, 200);
    return r.json();
  };
  const command = async (phase) => {
    const ack = Promise.race([
      once(lines, "line"),
      once(fixture, "exit").then(([code]) => {
        throw new Error(`Fixture exited: ${code}`);
      }),
    ]);
    fixture.stdin.write(JSON.stringify({ phase }) + "\n");
    assert.equal(JSON.parse((await ack)[0]).phase, phase);
  };
  const until = async (predicate) => {
    const end = Date.now() + 8000;
    while (Date.now() < end) {
      const data = await read("/api/work");
      if (predicate(data)) return data;
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error("Lifecycle observation did not converge");
  };
  const initial = await read("/api/work");
  assert.equal(initial.items.length, 2);
  assert.equal(initial.counts.queued, 1);
  assert.equal(initial.items.find((i) => i.number === 2).stage, "upcoming");
  assert.equal(initial.accounts.length, 1);
  assert(!JSON.stringify(initial).includes("FIXTURE_"));
  await command("start");
  const running = await until((d) => d.counts.running === 1);
  const work = running.items.find((i) => i.number === 3);
  assert.equal(work.stage, "in_progress");
  assert.equal(running.workers.length, 1);
  const detail = await read("/api/work/" + work.id);
  assert.equal(detail.attempts[0].taskId, taskId);
  socket = new WebSocket(
    base.replace("http:", "ws:") +
      `/ws/task-output?worker_id=vm-e2e&task_id=${taskId}&prefix=fleet-e2e`,
    { headers: { Cookie: cookie } },
  );
  await new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error("Output not received")),
      5000,
    );
    socket.on("message", (raw) => {
      if (
        JSON.parse(raw).lines?.some((s) =>
          s.includes("E2E agent output received"),
        )
      ) {
        clearTimeout(timer);
        resolve();
      }
    });
    socket.on("error", reject);
  });
  await command("finish");
  const waiting = await until(
    (d) => d.items.find((i) => i.number === 3)?.reason === "Waiting for CI",
  );
  assert.equal(waiting.items.find((i) => i.number === 3).stage, "in_progress");
  assert.equal(waiting.counts.done, 0);
  assert.equal(waiting.items.filter((i) => i.number === 7).length, 0);
  await command("merge");
  const completed = await until((d) => d.counts.done === 1);
  assert.equal(completed.items.find((i) => i.number === 3).stage, "done");
  assert.equal(
    (await read("/api/work?project=missing/project")).items.length,
    0,
  );
  assert.equal(
    (
      await fetch(base + "/api/auth/logout", {
        method: "POST",
        headers: { Cookie: cookie },
      })
    ).status,
    200,
  );
  assert.equal(
    (await fetch(base + "/api/work", { headers: { Cookie: cookie } })).status,
    401,
  );
  console.log(
    "Fleet E2E passed: Python writers → Redis protocol → authenticated API → live output → CI waiting → verified merge → sign-out.",
  );
} finally {
  socket?.terminate();
  await instance.close();
  fixture.stdin.end();
  fixture.kill();
  lines.close();
  redisProcess.kill();
  await once(redisProcess, "exit");
  await rm(redisDir, { recursive: true, force: true });
}
