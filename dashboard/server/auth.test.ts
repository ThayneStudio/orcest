// dashboard/server/auth.test.ts
// Run with: cd dashboard && npm ci && npm test   (vitest)
// NOTE: this is a TypeScript/vitest test, NOT a Python pytest test.
// The auth bug lives in TS; tests/test_dashboard.py exercises the unrelated
// Python `orcest.dashboard` module and cannot pin this regression (it adds a
// coarse source-text guard instead).
import { describe, it, expect, afterEach } from "vitest";
import type { IncomingMessage } from "http";
import { isAuthorized, tokenMatches } from "./auth.js";

function req(headers: Record<string, string> = {}, url = "/api/workers"): IncomingMessage {
  return { headers, url } as unknown as IncomingMessage;
}

const originalToken = process.env.DASHBOARD_TOKEN;
afterEach(() => {
  if (originalToken === undefined) delete process.env.DASHBOARD_TOKEN;
  else process.env.DASHBOARD_TOKEN = originalToken;
});

describe("isAuthorized", () => {
  it("fails closed when DASHBOARD_TOKEN is unset", () => {
    delete process.env.DASHBOARD_TOKEN;
    // No header, no token configured -> must be DENIED (regression: used to return true).
    expect(isAuthorized(req())).toBe(false);
    // Even a request that *presents* a bearer token must be denied when the
    // server has no token to compare against.
    expect(isAuthorized(req({ authorization: "Bearer anything" }))).toBe(false);
  });

  it("authorizes a correct Bearer token when DASHBOARD_TOKEN is set", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    expect(isAuthorized(req({ authorization: "Bearer s3cret" }))).toBe(true);
  });

  it("rejects a wrong Bearer token when DASHBOARD_TOKEN is set", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    expect(isAuthorized(req({ authorization: "Bearer nope" }))).toBe(false);
    expect(isAuthorized(req())).toBe(false);
  });

  it("authorizes via ?token= query param (WebSocket handshake path)", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    expect(isAuthorized(req({}, "/ws/snapshot?token=s3cret"))).toBe(true);
    expect(isAuthorized(req({}, "/ws/snapshot?token=wrong"))).toBe(false);
  });

  it("tokenMatches stays closed when no token configured", () => {
    delete process.env.DASHBOARD_TOKEN;
    expect(tokenMatches("whatever")).toBe(false);
  });
});
