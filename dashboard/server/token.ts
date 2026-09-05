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
