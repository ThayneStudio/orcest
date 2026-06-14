import type { IncomingMessage } from "http";
import { timingSafeEqual, createHash } from "crypto";

// Read the token lazily on every check so tests can set/clear
// process.env.DASHBOARD_TOKEN per case without module-reset hacks.
export function tokenMatches(candidate: string): boolean {
  const token = process.env.DASHBOARD_TOKEN;
  if (!token) return false;
  const a = createHash("sha256").update(candidate).digest();
  const b = createHash("sha256").update(token).digest();
  return timingSafeEqual(a, b);
}

export function isAuthorized(req: IncomingMessage): boolean {
  const token = process.env.DASHBOARD_TOKEN;
  // Fail CLOSED: with no configured token there is no way to authenticate,
  // so deny every request rather than authorize them all.
  if (!token) return false;
  const auth = (req as { headers: Record<string, string | string[] | undefined> }).headers
    .authorization;
  if (typeof auth === "string" && auth.startsWith("Bearer ") && tokenMatches(auth.slice(7)))
    return true;
  // Fallback to query-param token for WebSocket connections — browsers cannot
  // set custom headers (Authorization) on the WebSocket handshake request.
  const url = new URL(req.url || "", `http://localhost`);
  const queryToken = url.searchParams.get("token");
  if (queryToken !== null && tokenMatches(queryToken)) return true;
  return false;
}
