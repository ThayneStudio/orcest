// dashboard/server/auth.test.ts
// Run with: cd dashboard && npm ci && npm test   (vitest)
// NOTE: this is a TypeScript/vitest test, NOT a Python pytest test.
// The auth bug lives in TS; tests/test_dashboard.py exercises the unrelated
// Python `orcest.dashboard` module and cannot pin this regression (it adds a
// coarse source-text guard instead).
import { describe, it, expect, afterEach } from "vitest";
import type { IncomingMessage } from "http";
import { authCookieHeader, isAuthorized, tokenMatches } from "./auth.js";

function req(
  headers: Record<string, string | string[]> = {},
  url = "/api/workers",
): IncomingMessage {
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

  it("trims query-param tokens before checking auth or setting cookies", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    expect(isAuthorized(req({}, "/ws/snapshot?token=%20s3cret%20"))).toBe(true);
    expect(authCookieHeader(req({}, "/?token=%20s3cret%20"))).toContain(
      "orcest_dashboard_token=s3cret",
    );
  });

  it("treats malformed query-token URLs as unauthenticated", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    expect(isAuthorized(req({}, "http://["))).toBe(false);
    expect(authCookieHeader(req({}, "http://["))).toBeNull();
  });

  it("authorizes browser follow-up requests via the dashboard auth cookie", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    expect(
      isAuthorized(
        req({ cookie: "orcest_dashboard_token=s3cret" }, "/assets/app.js"),
      ),
    ).toBe(true);
    expect(
      isAuthorized(
        req({ cookie: "orcest_dashboard_token=wrong" }, "/assets/app.js"),
      ),
    ).toBe(false);
  });

  it("sets a scoped auth cookie when a valid query token is used", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const header = authCookieHeader(req({}, "/?token=s3cret"));

    expect(header).toContain("orcest_dashboard_token=s3cret");
    expect(header).toContain("HttpOnly");
    expect(header).toContain("SameSite=Strict");
    expect(authCookieHeader(req({}, "/?token=wrong"))).toBeNull();
  });

  it("ignores raw forwarded-proto headers without a trusted Express transport", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";

    expect(
      authCookieHeader(
        req({ "x-forwarded-proto": "https,http" }, "/?token=s3cret"),
      ),
    ).not.toContain("Secure");
    expect(
      authCookieHeader(
        req({ "x-forwarded-proto": ["https"] }, "/?token=s3cret"),
      ),
    ).not.toContain("Secure");
  });

  it("marks cookies Secure on a direct TLS connection", () => {
    process.env.DASHBOARD_TOKEN = "s3cret";
    const request = req({}, "/?token=s3cret");
    Object.assign(request, { socket: { encrypted: true } });
    expect(authCookieHeader(request)).toContain("Secure");
  });

  it("tokenMatches stays closed when no token configured", () => {
    delete process.env.DASHBOARD_TOKEN;
    expect(tokenMatches("whatever")).toBe(false);
  });
});
