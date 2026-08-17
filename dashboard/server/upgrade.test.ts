import { afterEach, describe, expect, it } from "vitest";
import type { IncomingMessage } from "http";
import { authCookieHeader } from "./auth.js";
import {
  assertValidDashboardAllowedOrigins,
  dashboardAllowedOriginConfig,
  dashboardUpgradeRejectionContext,
  resolveDashboardUpgrade,
} from "./upgrade.js";

function req(url: string, headers: Record<string, string> = {}): IncomingMessage {
  return { headers, url } as unknown as IncomingMessage;
}

const originalToken = process.env.DASHBOARD_TOKEN;
const originalAllowedOrigins = process.env.DASHBOARD_ALLOWED_ORIGINS;
afterEach(() => {
  if (originalToken === undefined) delete process.env.DASHBOARD_TOKEN;
  else process.env.DASHBOARD_TOKEN = originalToken;
  if (originalAllowedOrigins === undefined) delete process.env.DASHBOARD_ALLOWED_ORIGINS;
  else process.env.DASHBOARD_ALLOWED_ORIGINS = originalAllowedOrigins;
});

describe("resolveDashboardUpgrade", () => {
  it("normalizes valid allowed origins and reports invalid entries", () => {
    const config = dashboardAllowedOriginConfig([
      "https://ops.example.test/",
      "https://console.example.test:443",
      "http://localhost:8080/path",
      "pve-test.lab.prefixa.net",
      "ftp://ops.example.test",
      " ",
    ].join(","));

    expect([...config.origins]).toEqual([
      "https://ops.example.test",
      "https://console.example.test",
      "http://localhost:8080",
    ]);
    expect(config.invalid).toEqual([
      "pve-test.lab.prefixa.net",
      "ftp://ops.example.test",
    ]);
  });

  it("throws a clear startup error for invalid allowed-origin entries", () => {
    expect(() => assertValidDashboardAllowedOrigins(
      "https://ops.example.test,pve-test.lab.prefixa.net",
    )).toThrow(
      "Invalid DASHBOARD_ALLOWED_ORIGINS entries: pve-test.lab.prefixa.net. " +
      "Use comma-separated http(s) origins, for example https://dashboard.example.test.",
    );
  });

  it("authorizes WebSocket upgrades with the cookie set by an initial token request", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const setCookie = authCookieHeader(req("/?token=s3cret"));
    expect(setCookie).not.toBeNull();

    const cookie = setCookie!.split(";")[0];
    expect(resolveDashboardUpgrade(req("/ws/snapshot", { cookie }), 8080)).toEqual({
      authorized: true,
      target: "snapshot",
    });
    expect(resolveDashboardUpgrade(
      req("/ws/task-output?worker_id=worker-1&task_id=task-1", { cookie }),
      8080,
    )).toEqual({
      authorized: true,
      target: "task-output",
    });
  });

  it("keeps query-token WebSocket auth as a fallback", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    expect(resolveDashboardUpgrade(req("/ws/snapshot?token=s3cret"), 8080)).toEqual({
      authorized: true,
      target: "snapshot",
    });
  });

  it("accepts browser WebSocket upgrades from the dashboard origin", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    expect(resolveDashboardUpgrade(
      req("/ws/snapshot?token=s3cret", {
        host: "dashboard.example.test",
        origin: "https://dashboard.example.test",
        "x-forwarded-proto": "https",
      }),
      8080,
    )).toEqual({
      authorized: true,
      target: "snapshot",
    });
  });

  it("accepts same-origin upgrades when proxy Host includes the default HTTPS port", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    expect(resolveDashboardUpgrade(
      req("/ws/snapshot?token=s3cret", {
        host: "dashboard.example.test:443",
        origin: "https://dashboard.example.test",
        "x-forwarded-proto": "https",
      }),
      8080,
    )).toEqual({
      authorized: true,
      target: "snapshot",
    });
  });

  it("accepts same-origin upgrades when proxy Host includes the default HTTP port", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    expect(resolveDashboardUpgrade(
      req("/ws/snapshot?token=s3cret", {
        host: "dashboard.example.test:80",
        origin: "http://dashboard.example.test",
        "x-forwarded-proto": "http",
      }),
      8080,
    )).toEqual({
      authorized: true,
      target: "snapshot",
    });
  });

  it("accepts browser WebSocket upgrades from an explicitly allowed origin", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    process.env.DASHBOARD_ALLOWED_ORIGINS = "https://ops.example.test";

    expect(resolveDashboardUpgrade(
      req("/ws/task-output?worker_id=worker-1&token=s3cret", {
        host: "dashboard.example.test",
        origin: "https://ops.example.test",
      }),
      8080,
    )).toEqual({
      authorized: true,
      target: "task-output",
    });
  });

  it("normalizes explicitly allowed origins before comparing them", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    process.env.DASHBOARD_ALLOWED_ORIGINS = [
      "https://ops.example.test/",
      "https://console.example.test:443",
    ].join(",");

    expect(resolveDashboardUpgrade(
      req("/ws/snapshot?token=s3cret", {
        host: "dashboard.example.test",
        origin: "https://ops.example.test",
      }),
      8080,
    )).toEqual({
      authorized: true,
      target: "snapshot",
    });
    expect(resolveDashboardUpgrade(
      req("/ws/task-output?worker_id=worker-1&token=s3cret", {
        host: "dashboard.example.test",
        origin: "https://console.example.test",
      }),
      8080,
    )).toEqual({
      authorized: true,
      target: "task-output",
    });
  });

  it("rejects browser WebSocket upgrades from untrusted origins", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    const request = req("/ws/snapshot?token=s3cret", {
        host: "dashboard.example.test",
        origin: "https://evil.example.test",
        "x-forwarded-proto": "https",
      });

    expect(resolveDashboardUpgrade(
      request,
      8080,
    )).toEqual({
      authorized: false,
      target: null,
      reason: "origin",
    });
    expect(dashboardUpgradeRejectionContext(request, 8080)).toEqual({
      path: "/ws/snapshot",
      origin: "https://evil.example.test",
      host: "dashboard.example.test",
      forwarded_proto: "https",
    });
  });

  it("rejects WebSocket upgrades without a valid token or cookie", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    expect(resolveDashboardUpgrade(req("/ws/snapshot"), 8080)).toEqual({
      authorized: false,
      target: null,
      reason: "auth",
    });
    expect(resolveDashboardUpgrade(
      req("/ws/task-output", { cookie: "orcest_dashboard_token=wrong" }),
      8080,
    )).toEqual({
      authorized: false,
      target: null,
      reason: "auth",
    });
  });

  it("authorizes but does not route unknown upgrade paths", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    expect(resolveDashboardUpgrade(req("/ws/unknown?token=s3cret"), 8080)).toEqual({
      authorized: true,
      target: null,
    });
  });

  it("does not throw when an authenticated upgrade has a malformed request target", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const setCookie = authCookieHeader(req("/?token=s3cret"));
    expect(setCookie).not.toBeNull();

    expect(resolveDashboardUpgrade(
      req("http://[", { cookie: setCookie!.split(";")[0] }),
      8080,
    )).toEqual({
      authorized: true,
      target: null,
    });
  });
});
