import type { RecentResult } from "./types";
import { resourceLabel, resourceTypeLabel } from "./resource";

export type ResultColumn = "completed" | "failed" | "neutral";

export function resultStatusLabel(status: string): string {
  switch (status.trim().toLowerCase()) {
    case "completed":
      return "Completed";
    case "failed":
      return "Failed";
    case "usage_exhausted":
      return "Usage exhausted";
    case "stale":
      return "Stale";
    default:
      return status.trim() || "unknown";
  }
}

export function resultTypeLabel(type: string): string {
  return resourceTypeLabel(type);
}

export function resultStreamResourcePrefix(resultStream: string | undefined): string | null {
  const trimmed = resultStream?.trim() || "";
  if (!trimmed || trimmed === "results") return null;
  const match = trimmed.match(/^(.+):results$/);
  return match?.[1]?.trim() || null;
}

export function resultEntryIdParts(entryId: string): [number, number] | null {
  const match = entryId.trim().match(/^(\d+)-(\d+)$/);
  if (!match) return null;
  const timestamp = Number(match[1]);
  const sequence = Number(match[2]);
  return Number.isSafeInteger(timestamp) &&
    timestamp >= 0 &&
    Number.isSafeInteger(sequence) &&
    sequence >= 0
    ? [timestamp, sequence]
    : null;
}

export function resultTimestampMs(entryId: string): number | null {
  return resultEntryIdParts(entryId)?.[0] ?? null;
}

function resultEntryIdCompare(a: string, b: string): number | null {
  const aParts = resultEntryIdParts(a);
  const bParts = resultEntryIdParts(b);
  if (!aParts || !bParts) return null;
  return aParts[0] - bParts[0] || aParts[1] - bParts[1];
}

export function latestResultByEntryId<T extends Pick<RecentResult, "entry_id">>(
  results: T[],
): T | null {
  let latest: T | null = null;

  for (const result of results) {
    if (!latest) {
      latest = result;
      continue;
    }

    const comparison = resultEntryIdCompare(result.entry_id, latest.entry_id);
    if (comparison === null) {
      if (resultEntryIdParts(result.entry_id) && !resultEntryIdParts(latest.entry_id)) {
        latest = result;
      }
      continue;
    }
    if (comparison > 0) {
      latest = result;
    }
  }

  return latest;
}

export function resultResourceLabel(
  result: Pick<RecentResult, "repo" | "resource_type" | "resource_id" | "result_stream">,
): string {
  return resourceLabel({
    ...result,
    prefix: resultStreamResourcePrefix(result.result_stream),
  });
}

export function resultColumnForStatus(status: string): ResultColumn {
  switch (status.trim().toLowerCase()) {
    case "completed":
      return "completed";
    case "failed":
    case "usage_exhausted":
      return "failed";
    case "stale":
    default:
      return "neutral";
  }
}

export function partitionResultsByStatus(results: RecentResult[]): {
  completed: RecentResult[];
  failed: RecentResult[];
  neutral: RecentResult[];
} {
  const completed: RecentResult[] = [];
  const failed: RecentResult[] = [];
  const neutral: RecentResult[] = [];

  for (const result of results) {
    switch (resultColumnForStatus(result.status)) {
      case "completed":
        completed.push(result);
        break;
      case "failed":
        failed.push(result);
        break;
      case "neutral":
        neutral.push(result);
        break;
    }
  }

  return { completed, failed, neutral };
}
