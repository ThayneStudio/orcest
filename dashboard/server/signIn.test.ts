import { afterEach, expect, it, vi } from "vitest";
import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { WebSocket } from "ws";
import {
  createDashboardServer,
  type DashboardServerInstance,
} from "./index.js";
import { sessionAuthorized } from "./signIn.js";
import type { IncomingMessage } from "node:http";
let instance: DashboardServerInstance | undefined;
afterEach(async () => {
  if (instance) await instance.close();
  vi.unstubAllEnvs();
});
it("signs in without leaking the token, protects HTTP and sockets, and revokes both on sign-out", async () => {
  vi.stubEnv("DASHBOARD_TOKEN", "private-login-test-token");
  instance = createDashboardServer({
    port: 0,
    deps: {
      redisQuit: async () => {},
      buildDashboardMessage: async () => ({
        snapshot: {} as never,
        workers: [],
        stuck_tasks: [],
      }),
      logInfo: () => {},
      logError: () => {},
    },
  });
  instance.server.listen(0, "127.0.0.1");
  await once(instance.server, "listening");
  const base = `http://127.0.0.1:${(instance.server.address() as AddressInfo).port}`;
  const request = (path: string, options: RequestInit = {}) =>
    fetch(base + path, options);
  const login = (token: string, headers: Record<string, string> = {}) =>
    request("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json", ...headers },
      body: JSON.stringify({ token }),
    });
  expect(
    (
      await request("/", {
        headers: { Accept: "text/html" },
        redirect: "manual",
      })
    ).headers.get("location"),
  ).toBe("/sign-in");
  const page = await request("/sign-in");
  expect(page.status).toBe(200);
  expect(await page.text()).toContain('type="password"');
  expect((await request("/api/snapshot")).status).toBe(401);
  expect(
    (
      await login("private-login-test-token", {
        Origin: "https://attacker.invalid",
      })
    ).status,
  ).toBe(403);
  expect((await login("incorrect")).status).toBe(401);
  const response = await login("private-login-test-token");
  expect(response.status).toBe(200);
  const setCookie = response.headers.get("set-cookie")!;
  expect(setCookie).toContain("HttpOnly");
  expect(setCookie).toContain("SameSite=Strict");
  expect(setCookie).not.toContain("private-login-test-token");
  const cookie = setCookie.split(";")[0];
  const headers = { Cookie: cookie };
  expect((await request("/api/snapshot", { headers })).status).toBe(200);
  const ws = new WebSocket(base.replace("http:", "ws:") + "/ws/snapshot", {
    headers,
  });
  await once(ws, "open");
  const closed = once(ws, "close");
  expect(
    (await request("/api/auth/logout", { method: "POST", headers })).status,
  ).toBe(200);
  expect((await closed)[0]).toBe(1008);
  expect((await request("/api/snapshot", { headers })).status).toBe(401);
  const fresh = await login("private-login-test-token", {
    "X-Forwarded-Proto": "https",
  });
  expect(fresh.headers.get("set-cookie")).toContain("Secure");
  const freshCookie = fresh.headers.get("set-cookie")!.split(";")[0];
  vi.stubEnv("DASHBOARD_TOKEN", "rotated-token");
  expect(
    sessionAuthorized({ headers: { cookie: freshCookie } } as IncomingMessage),
  ).toBe(false);
  vi.stubEnv("DASHBOARD_TOKEN", "");
  expect((await login("any")).status).toBe(503);
  vi.stubEnv("DASHBOARD_TOKEN", "another-token");
  for (let i = 0; i < 10; i++) expect((await login("wrong")).status).toBe(401);
  const limited = await login("another-token");
  expect(limited.status).toBe(429);
  expect(limited.headers.get("retry-after")).toBe("60");
});
