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

export function snapshotUpdateDate(
  fetchedAt: string | null | undefined,
  receivedAt = new Date(),
): Date {
  if (typeof fetchedAt !== "string" || fetchedAt.trim() === "") {
    return receivedAt;
  }

  const parsed = new Date(fetchedAt);
  return Number.isFinite(parsed.getTime()) ? parsed : receivedAt;
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
