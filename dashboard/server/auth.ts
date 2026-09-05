import type { IncomingMessage } from "http";
import { tokenMatches } from "./token.js";
export { tokenMatches } from "./token.js";

import { sessionAuthorized } from "./signIn.js";

const AUTH_COOKIE = "orcest_dashboard_token";

function tokenFromQuery(req: IncomingMessage): string | null {
  let url: URL;
  try {
    url = new URL(req.url || "", `http://localhost`);
  } catch {
    return null;
  }
  const token = url.searchParams.get("token")?.trim() || "";
  return token || null;
}

function tokenFromCookie(req: IncomingMessage): string | null {
  const cookie = req.headers.cookie;
  if (typeof cookie !== "string") return null;

  for (const part of cookie.split(";")) {
    const [rawName, ...rawValue] = part.trim().split("=");
    if (rawName !== AUTH_COOKIE) continue;
    const value = rawValue.join("=");
    if (!value) return null;
    try {
      return decodeURIComponent(value);
    } catch {
      return value;
    }
  }

  return null;
}

function firstHeaderValue(value: string | string[] | undefined): string {
  return (Array.isArray(value) ? value[0] : value) || "";
}

function forwardedProto(req: IncomingMessage): string {
  return firstHeaderValue(req.headers["x-forwarded-proto"])
    .split(",")[0]
    .trim()
    .toLowerCase();
}

export function isAuthorized(req: IncomingMessage): boolean {
  const token = process.env.DASHBOARD_TOKEN;
  // Fail CLOSED: with no configured token there is no way to authenticate,
  // so deny every request rather than authorize them all.
  if (!token) return false;
  if (sessionAuthorized(req)) return true;
  const auth = (req as { headers: Record<string, string | string[] | undefined> }).headers
    .authorization;
  if (typeof auth === "string" && auth.startsWith("Bearer ") && tokenMatches(auth.slice(7)))
    return true;
  const cookieToken = tokenFromCookie(req);
  if (cookieToken !== null && tokenMatches(cookieToken)) return true;
  // Fallback to query-param token for WebSocket connections — browsers cannot
  // set custom headers (Authorization) on the WebSocket handshake request.
  const queryToken = tokenFromQuery(req);
  if (queryToken !== null && tokenMatches(queryToken)) return true;
  return false;
}

export function authCookieHeader(req: IncomingMessage): string | null {
  const queryToken = tokenFromQuery(req);
  if (queryToken === null || !tokenMatches(queryToken)) return null;

  const encoded = encodeURIComponent(queryToken);
  const secure =
    Boolean((req.socket as { encrypted?: boolean } | undefined)?.encrypted) ||
    forwardedProto(req) === "https";
  return [
    `${AUTH_COOKIE}=${encoded}`,
    "Path=/",
    "HttpOnly",
    "SameSite=Strict",
    "Max-Age=604800",
    secure ? "Secure" : "",
  ].filter(Boolean).join("; ");
}
