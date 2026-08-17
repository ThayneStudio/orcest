import { degradedSectionsSummary } from "../lib/degradedSections";

export {
  degradedSectionLabel,
  degradedSectionsSummary,
  hasDegradedSection,
  normalizeDegradedSection,
  normalizedDegradedSectionSet,
} from "../lib/degradedSections";

export function SnapshotDegraded({ sections }: { sections: string[] }) {
  const summary = degradedSectionsSummary(sections);
  if (!summary) return null;
  const fullSummary = degradedSectionsSummary(sections, Number.POSITIVE_INFINITY);

  return (
    <div
      role="status"
      aria-live="polite"
      className="border border-yellow-500/30 bg-yellow-500/10 px-4 py-3 text-sm text-yellow-100"
    >
      <div className="font-medium text-yellow-200">Dashboard data is partially unavailable</div>
      <div className="mt-1 break-words text-yellow-100/80" title={fullSummary}>
        Incomplete sections: {summary}
      </div>
    </div>
  );
}
