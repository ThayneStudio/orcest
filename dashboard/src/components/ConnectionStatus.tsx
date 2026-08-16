import { useEffect, useState } from "react";
import {
  snapshotConnectionStatus,
  snapshotDisplayTime,
  snapshotServerTimeLabel,
} from "../lib/connection";

interface Props {
  connected: boolean;
  error?: string | null;
  /** Browser clock at snapshot receipt. Freshness is measured against this only. */
  lastUpdate: Date | null;
  /** Server-stamped `snapshot.fetched_at`, surfaced in the tooltip for diagnosis. */
  serverFetchedAt?: Date | null;
}

function dotClass(kind: string): string {
  switch (kind) {
    case "connected":
      return "bg-emerald-400";
    case "waiting":
    case "stale":
      return "bg-yellow-400 animate-pulse";
    default:
      return "bg-red-400 animate-pulse";
  }
}

function labelClass(kind: string): string {
  switch (kind) {
    case "connected":
      return "text-zinc-400";
    case "waiting":
    case "stale":
      return "text-yellow-400";
    default:
      return "text-red-400";
  }
}

function detailClass(kind: string): string {
  return kind === "stale" ? "text-yellow-300" : "text-red-300";
}

export function ConnectionStatus({ connected, error, lastUpdate, serverFetchedAt }: Props) {
  const [nowMs, setNowMs] = useState(() => Date.now());

  useEffect(() => {
    setNowMs(Date.now());
    if (!lastUpdate) return;
    const timer = setInterval(() => setNowMs(Date.now()), 5000);
    return () => clearInterval(timer);
  }, [lastUpdate]);

  const status = snapshotConnectionStatus(connected, error, lastUpdate, nowMs);
  const displayTime = snapshotDisplayTime(lastUpdate);
  const serverTimeLabel = snapshotServerTimeLabel(serverFetchedAt);
  const announcementDetail = status.announcement.startsWith(`${status.label}: `)
    ? status.announcement.slice(status.label.length + 2)
    : "";

  return (
    <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 text-sm sm:justify-end">
      <span
        aria-hidden="true"
        className={`inline-block h-2 w-2 shrink-0 rounded-full ${dotClass(status.kind)}`}
      />
      <span
        role="status"
        aria-live="polite"
        aria-atomic="true"
        className={`shrink-0 ${labelClass(status.kind)}`}
      >
        {status.label}
        {announcementDetail && <span className="sr-only">: {announcementDetail}</span>}
      </span>
      {status.detail && (
        <span
          className={`min-w-0 max-w-full truncate text-xs sm:max-w-[28rem] ${detailClass(status.kind)}`}
          title={status.detail}
        >
          {status.detail}
        </span>
      )}
      {displayTime && (
        <span className="shrink-0 text-zinc-500" title={serverTimeLabel ?? undefined}>
          {displayTime}
        </span>
      )}
    </div>
  );
}
