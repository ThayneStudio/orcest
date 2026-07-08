import { degradedCountLabel, loadedCountLabel } from "../lib/counts";
import { formatDuration, formatTimestampMs } from "../lib/format";
import {
  latestResultByEntryId,
  partitionResultsByStatus,
  resultResourceLabel,
  resultStatusLabel,
  resultTimestampMs,
} from "../lib/results";
import type { RecentResult } from "../lib/types";

type ResultHealthFilter = "all" | "completed" | "failed" | "neutral";

interface Props {
  results: RecentResult[];
  total?: number;
  degraded?: boolean;
  depthDegraded?: boolean;
  onOpenResults?: (filter?: ResultHealthFilter) => void;
}

export function resultHealthLoadedLabel(
  loaded: number,
  total = loaded,
  depthDegraded = false,
): string {
  if (depthDegraded) return loaded > 0 ? `${loaded} loaded, ? total` : "? total";
  const normalizedTotal = Math.max(loaded, total);
  return normalizedTotal > loaded
    ? `${loaded} loaded, ${normalizedTotal} total`
    : `${loaded} loaded`;
}

export function resultHealthStatusLabel(
  attentionCount: number,
  degraded: boolean,
  depthDegraded = false,
): string {
  const noun = attentionCount === 1 ? "result needs" : "results need";
  if (degraded || depthDegraded) {
    return attentionCount > 0
      ? `${attentionCount} loaded ${noun} attention, partial data`
      : "partial data";
  }
  if (attentionCount > 0) {
    return `${attentionCount} loaded ${noun} attention`;
  }
  return "no loaded results need attention";
}

export function resultHealthEmptyMessage(degraded: boolean): string {
  return degraded ? "Recent results unavailable" : "No recent results loaded";
}

function countCardTone(kind: "completed" | "failed" | "neutral", count: number): string {
  if (kind === "completed") return "border-emerald-500/20 text-emerald-300";
  if (kind === "failed" && count > 0) return "border-red-500/30 text-red-300";
  return "border-zinc-800 text-zinc-300";
}

export function resultHealthCountActionLabel(
  filter: ResultHealthFilter,
  count: number,
  degraded = false,
): string {
  const countLabel = loadedCountLabel(count, degraded);
  switch (filter) {
    case "all":
      return `View all results, ${countLabel}`;
    case "completed":
      return `View completed results, ${countLabel}`;
    case "failed":
      return `View results that need attention, ${countLabel}`;
    case "neutral":
      return `View other results, ${countLabel}`;
  }
}

export function resultHealthCountDisplayLabel(
  count: number,
  loadedPreviewOnly = false,
  degraded = false,
): string {
  return loadedPreviewOnly
    ? loadedCountLabel(count, degraded)
    : degradedCountLabel(count, degraded);
}

function statusClass(attentionCount: number, degraded: boolean): string {
  if (attentionCount > 0) return "text-red-300";
  if (degraded) return "text-yellow-300";
  return "text-zinc-500";
}

export function ResultHealth({
  results,
  total = results.length,
  degraded = false,
  depthDegraded = false,
  onOpenResults,
}: Props) {
  const { completed, failed, neutral } = partitionResultsByStatus(results);
  const latestAttention = latestResultByEntryId(failed);
  const status = resultHealthStatusLabel(failed.length, degraded, depthDegraded);
  const countDepthDegraded = depthDegraded || (degraded && results.length === 0 && total === 0);
  const countCardsShowLoaded = countDepthDegraded || degraded || total > results.length;
  const counts = [
    { key: "completed" as const, label: "Completed", count: completed.length },
    { key: "failed" as const, label: "Needs attention", count: failed.length },
    { key: "neutral" as const, label: "Other", count: neutral.length },
  ];

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h2 className="text-sm font-medium text-zinc-400">
          Recent Result Health
        </h2>
        <div className="flex flex-wrap items-center gap-2 text-xs">
          <span className="text-zinc-500">
            {resultHealthLoadedLabel(results.length, total, countDepthDegraded)}
          </span>
          <span className={statusClass(failed.length, degraded || depthDegraded)}>{status}</span>
          {onOpenResults && (
            <button
              type="button"
              className="rounded border border-zinc-700 px-2 py-1 text-zinc-300 hover:border-zinc-600 hover:text-zinc-100 focus:outline-none focus:ring-2 focus:ring-zinc-500"
              onClick={() => onOpenResults()}
            >
              View Results
            </button>
          )}
        </div>
      </div>

      {results.length === 0 ? (
        <div className="rounded-lg border border-zinc-800 bg-zinc-900 px-4 py-3 text-sm text-zinc-500">
          {resultHealthEmptyMessage(degraded)}
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,1fr)_minmax(20rem,1.5fr)]">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
            {counts.map((row) => (
              onOpenResults ? (
                <button
                  key={row.key}
                  type="button"
                  aria-label={resultHealthCountActionLabel(row.key, row.count, degraded)}
                  className={`min-w-0 rounded-lg border bg-zinc-900 px-3 py-3 text-left transition-colors hover:border-zinc-600 focus:outline-none focus:ring-2 focus:ring-zinc-500 ${countCardTone(row.key, row.count)}`}
                  onClick={() => onOpenResults(row.key)}
                >
                  <div className="break-words text-xs text-zinc-500">{row.label}</div>
                  <div
                    className="min-w-0 break-all font-mono text-2xl font-bold"
                    title={resultHealthCountDisplayLabel(row.count, countCardsShowLoaded, degraded)}
                  >
                    {resultHealthCountDisplayLabel(row.count, countCardsShowLoaded, degraded)}
                  </div>
                </button>
              ) : (
                <div
                  key={row.key}
                  className={`min-w-0 rounded-lg border bg-zinc-900 px-3 py-3 ${countCardTone(row.key, row.count)}`}
                >
                  <div className="break-words text-xs text-zinc-500">{row.label}</div>
                  <div
                    className="min-w-0 break-all font-mono text-2xl font-bold"
                    title={resultHealthCountDisplayLabel(row.count, countCardsShowLoaded, degraded)}
                  >
                    {resultHealthCountDisplayLabel(row.count, countCardsShowLoaded, degraded)}
                  </div>
                </div>
              )
            ))}
          </div>

          {latestAttention ? (
            <div className="min-w-0 rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3">
              <div className="mb-2 flex min-w-0 flex-wrap items-center justify-between gap-2">
                <div className="min-w-0 truncate font-mono text-sm text-red-200" title={resultResourceLabel(latestAttention)}>
                  {resultResourceLabel(latestAttention)}
                </div>
                <span className="shrink-0 rounded-full bg-red-500/20 px-2 py-0.5 text-xs text-red-300">
                  {resultStatusLabel(latestAttention.status)}
                </span>
              </div>
              <div className="mb-1 flex min-w-0 flex-wrap gap-x-3 gap-y-1 text-xs text-red-200/70">
                <span title={`entry ${latestAttention.entry_id}`}>
                  {formatTimestampMs(resultTimestampMs(latestAttention.entry_id))}
                </span>
                <span className="min-w-0 truncate" title={latestAttention.worker_id}>
                  {latestAttention.worker_id}
                </span>
                <span>{formatDuration(latestAttention.duration_seconds)}</span>
              </div>
              <div className="line-clamp-2 break-words text-sm text-red-100/80" title={latestAttention.summary || undefined}>
                {latestAttention.summary || "(no summary)"}
              </div>
            </div>
          ) : (
            <div className="rounded-lg border border-zinc-800 bg-zinc-900 px-4 py-3 text-sm text-zinc-500">
              No loaded results need attention
            </div>
          )}
        </div>
      )}
    </div>
  );
}
