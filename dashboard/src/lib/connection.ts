import { abnormalCloseMessage } from "./websocket";

export const SNAPSHOT_STALE_AFTER_MS = 15_000;
export const SNAPSHOT_ABNORMAL_CLOSE_THRESHOLD = 1;
export const SNAPSHOT_RECONNECT_ABNORMAL_CLOSE_THRESHOLD = 5;
export const SNAPSHOT_PROTOCOL_ERROR =
  "Dashboard snapshot stream sent malformed data.";

export type SnapshotConnectionKind = "connected" | "waiting" | "stale" | "issue" | "disconnected";

export interface SnapshotConnectionStatus {
  kind: SnapshotConnectionKind;
  label: string;
  detail: string | null;
  announcement: string;
}

/**
 * Parse the SERVER-generated `snapshot.fetched_at` into a Date, for DISPLAY ONLY.
 *
 * Never feed this into staleness math. `fetched_at` is stamped by the dashboard
 * server process (`new Date().toISOString()`), while every freshness comparison
 * here runs against `Date.now()` in the browser, and the two clocks are never
 * reconciled. If the server clock runs ahead, snapshots arrive "in the future",
 * the `Math.max(0, …)` clamp in `snapshotAgeSeconds` pins the age at 0, and a
 * hung feed on a still-open socket would read "Connected" forever; if it runs
 * behind, a perfectly healthy feed reads "Stale snapshot" permanently. Staleness
 * is measured from the receipt timestamp the client stamps in `ws.onmessage`,
 * which is self-consistent and skew-immune.
 */
export function snapshotFetchedDate(fetchedAt: string | null | undefined): Date | null {
  if (typeof fetchedAt !== "string" || fetchedAt.trim() === "") {
    return null;
  }

  const parsed = new Date(fetchedAt);
  return Number.isFinite(parsed.getTime()) ? parsed : null;
}

/** Tooltip text disclosing the server-side snapshot time next to the local receipt time. */
export function snapshotServerTimeLabel(
  serverFetchedAt: Date | null | undefined,
): string | null {
  if (!serverFetchedAt || !Number.isFinite(serverFetchedAt.getTime())) return null;
  return (
    `Snapshot generated ${serverFetchedAt.toISOString()} (server clock). ` +
    "Age is measured from browser receipt time."
  );
}

export function snapshotAgeSeconds(lastUpdate: Date | null, nowMs = Date.now()): number | null {
  if (!lastUpdate) return null;
  const updatedMs = lastUpdate.getTime();
  if (!Number.isFinite(updatedMs)) return null;
  return Math.max(0, Math.floor((nowMs - updatedMs) / 1000));
}

export function formatSnapshotAge(seconds: number | null): string | null {
  if (seconds === null) return null;
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (minutes < 60) return secs === 0 ? `${minutes}m ago` : `${minutes}m ${secs}s ago`;
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return mins === 0 ? `${hours}h ago` : `${hours}h ${mins}m ago`;
}

export function snapshotDisplayTime(lastUpdate: Date | null): string | null {
  if (!lastUpdate) return null;
  return Number.isFinite(lastUpdate.getTime()) ? lastUpdate.toLocaleTimeString() : null;
}

export function snapshotConnectionStatus(
  connected: boolean,
  error: string | null | undefined,
  lastUpdate: Date | null,
  nowMs = Date.now(),
  staleAfterMs = SNAPSHOT_STALE_AFTER_MS,
): SnapshotConnectionStatus {
  const ageSeconds = snapshotAgeSeconds(lastUpdate, nowMs);
  const ageDetail = formatSnapshotAge(ageSeconds);
  const errorDetail = error?.trim() || null;
  if (!connected) {
    if (!errorDetail && ageDetail === null) {
      return {
        kind: "waiting",
        label: "Connecting",
        detail: null,
        announcement: "Connecting",
      };
    }
    const detail = ageDetail ? `Last update ${ageDetail}` : null;
    return errorDetail
      ? {
        kind: "issue",
        label: "Connection issue",
        detail: detail ? `${errorDetail} (${detail})` : errorDetail,
        announcement: `Connection issue: ${errorDetail}`,
      }
      : { kind: "disconnected", label: "Disconnected", detail, announcement: "Disconnected" };
  }

  if (!lastUpdate || ageSeconds === null) {
    return {
      kind: "waiting",
      label: "Waiting for snapshot",
      detail: null,
      announcement: "Waiting for snapshot",
    };
  }

  if (lastUpdate && ageSeconds !== null && ageSeconds * 1000 >= staleAfterMs) {
    return {
      kind: "stale",
      label: "Stale snapshot",
      detail: `Last update ${ageDetail}`,
      announcement: "Stale snapshot",
    };
  }

  return { kind: "connected", label: "Connected", detail: null, announcement: "Connected" };
}

export function snapshotCloseErrorMessage(
  code: number,
  consecutiveFailures: number,
  threshold = SNAPSHOT_ABNORMAL_CLOSE_THRESHOLD,
): string | null {
  return abnormalCloseMessage(code, consecutiveFailures, threshold, "Dashboard");
}
