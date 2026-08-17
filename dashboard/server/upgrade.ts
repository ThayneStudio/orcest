import type { IncomingMessage } from "http";
import { isAuthorized } from "./auth.js";

export type DashboardUpgradeTarget = "snapshot" | "task-output";
export type DashboardUpgradeDenialReason = "auth" | "origin";

export type DashboardUpgradeDecision =
  | { authorized: true; target: DashboardUpgradeTarget | null }
  | { authorized: false; target: null; reason: DashboardUpgradeDenialReason };

export interface DashboardUpgradeRejectionContext {
  path: string | null;
  origin: string | null;
  host: string | null;
  forwarded_proto: string | null;
}

export interface DashboardAllowedOriginConfig {
  origins: Set<string>;
  invalid: string[];
}

function headerValue(value: string | string[] | undefined): string | null {
  if (Array.isArray(value)) return value[0] || null;
  return value || null;
}

function originFor(protocol: string, host: string): string | null {
  try {
    return new URL(`${protocol}://${host}`).origin;
  } catch {
    return null;
  }
}

function normalizeAllowedOrigin(value: string): string | null {
  try {
    const url = new URL(value);
    if (url.protocol !== "http:" && url.protocol !== "https:") return null;
    return url.origin;
  } catch {
    return null;
  }
}

function requestPathname(req: IncomingMessage, port: number): string | null {
  try {
    return new URL(req.url || "", `http://localhost:${port}`).pathname;
  } catch {
    return null;
  }
}

export function dashboardAllowedOriginConfig(
  raw = process.env.DASHBOARD_ALLOWED_ORIGINS || "",
): DashboardAllowedOriginConfig {
  const origins = new Set<string>();
  const invalid: string[] = [];

  for (const entry of raw.split(",")) {
    const trimmed = entry.trim();
    if (!trimmed) continue;

    const origin = normalizeAllowedOrigin(trimmed);
    if (origin) origins.add(origin);
    else invalid.push(trimmed);
  }

  return { origins, invalid };
}

export function assertValidDashboardAllowedOrigins(
  raw = process.env.DASHBOARD_ALLOWED_ORIGINS || "",
): void {
  const { invalid } = dashboardAllowedOriginConfig(raw);
  if (invalid.length === 0) return;
  throw new Error(
    "Invalid DASHBOARD_ALLOWED_ORIGINS entries: " +
    `${invalid.join(", ")}. Use comma-separated http(s) origins, ` +
    "for example https://dashboard.example.test.",
  );
}

export function isTrustedDashboardOrigin(req: IncomingMessage): boolean {
  const originHeader = headerValue(req.headers.origin);
  if (!originHeader) return true;

  let origin: URL;
  try {
    origin = new URL(originHeader);
  } catch {
    return false;
  }
  const normalizedOrigin = origin.origin;

  if (dashboardAllowedOriginConfig().origins.has(normalizedOrigin)) return true;

  const host = headerValue(req.headers.host);
  if (!host) return false;

  const forwardedProto = headerValue(req.headers["x-forwarded-proto"])
    ?.split(",")[0]
    ?.trim();
  const protocol =
    forwardedProto ||
    (Boolean((req.socket as { encrypted?: boolean } | undefined)?.encrypted) ? "https" : null);

  if (protocol) return normalizedOrigin === originFor(protocol, host);
  return (
    normalizedOrigin === originFor("http", host) ||
    normalizedOrigin === originFor("https", host)
  );
}

export function resolveDashboardUpgrade(
  req: IncomingMessage,
  port: number,
): DashboardUpgradeDecision {
  if (!isAuthorized(req)) return { authorized: false, target: null, reason: "auth" };
  if (!isTrustedDashboardOrigin(req)) return { authorized: false, target: null, reason: "origin" };

  const pathname = requestPathname(req, port);
  if (pathname === "/ws/snapshot") return { authorized: true, target: "snapshot" };
  if (pathname === "/ws/task-output") return { authorized: true, target: "task-output" };
  return { authorized: true, target: null };
}

export function dashboardUpgradeRejectionContext(
  req: IncomingMessage,
  port: number,
): DashboardUpgradeRejectionContext {
  return {
    path: requestPathname(req, port),
    origin: headerValue(req.headers.origin),
    host: headerValue(req.headers.host),
    forwarded_proto: headerValue(req.headers["x-forwarded-proto"]),
  };
}
