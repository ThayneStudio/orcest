const DEGRADED_SECTION_LABELS: Record<string, string> = {
  "active locks": "Active work",
  "attempt counts": "Retry counters",
  "consumer groups": "Consumer groups",
  "dead letter depth": "Dead-letter count",
  "dead letter entries": "Dead-letter entries",
  "provider health": "Provider health",
  "queue depths": "Task stream depths",
  "queued tasks": "Queued tasks",
  "recent results": "Recent results",
  "results depth": "Results depth",
  "stuck tasks": "Stuck task detection",
  "task output prefixes": "Task output lookup",
  "worker discovery": "Worker output discovery",
  "worker pool": "Worker pool",
};

export function normalizeDegradedSection(section: string): string {
  return section.trim().toLowerCase().replace(/[-_]+/g, " ").replace(/\s+/g, " ");
}

export function normalizedDegradedSectionSet(sections: string[]): Set<string> {
  return new Set(sections.map(normalizeDegradedSection).filter(Boolean));
}

export function hasDegradedSection(sections: Set<string>, section: string): boolean {
  return sections.has(normalizeDegradedSection(section));
}

export function degradedSectionLabel(section: string): string {
  const normalized = normalizeDegradedSection(section);
  if (!normalized) return "";
  return DEGRADED_SECTION_LABELS[normalized] ?? normalized;
}

export function degradedSectionsSummary(sections: string[], limit = 4): string {
  const seen = new Set<string>();
  const labels: string[] = [];
  for (const section of sections) {
    const label = degradedSectionLabel(section);
    const key = label.toLowerCase();
    if (!label || seen.has(key)) continue;
    seen.add(key);
    labels.push(label);
  }
  const visible = labels.slice(0, limit);
  const hiddenCount = labels.length - visible.length;
  if (hiddenCount <= 0) return visible.join(", ");
  return `${visible.join(", ")}, and ${hiddenCount} more`;
}
