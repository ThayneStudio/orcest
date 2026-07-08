export function isTaskStreamName(name: string): boolean {
  return taskStreamParts(name) !== null;
}

type TaskStreamParts = {
  prefix: string;
  qualifier: string;
  provider: string;
};

function taskStreamParts(name: string): TaskStreamParts | null {
  const parts = name.trim().split(":");
  if (parts.some((part) => part === "")) return null;
  const taskIndex = parts.indexOf("tasks");
  if (taskIndex < 0 || taskIndex === parts.length - 1) return null;

  const suffix = parts.slice(taskIndex + 1);
  if (suffix.length === 0) return null;

  const provider = suffix.length === 1 ? suffix[0] : suffix.slice(1).join(":");
  if (!provider) return null;

  return {
    prefix: parts.slice(0, taskIndex).join(":"),
    qualifier: suffix.length > 1 ? suffix[0] : "",
    provider,
  };
}

function prefixedTerminalStreamDisplayName(name: string, terminal: string): string | null {
  if (name === terminal) return terminal;
  const suffix = `:${terminal}`;
  if (!name.endsWith(suffix)) return null;

  const prefix = name.slice(0, -suffix.length);
  return prefix ? `[${prefix}] ${terminal}` : terminal;
}

function taskStreamDisplayName(name: string): string | null {
  const parts = taskStreamParts(name);
  if (!parts) return null;

  const taskLabel = [parts.qualifier, parts.provider].filter(Boolean).join(" ");
  return parts.prefix ? `[${parts.prefix}] ${taskLabel}` : taskLabel;
}

export function redisStreamDisplayName(name: string): string {
  const trimmed = name.trim();
  if (!trimmed) return "?";

  return taskStreamDisplayName(trimmed) ??
    prefixedTerminalStreamDisplayName(trimmed, "results") ??
    prefixedTerminalStreamDisplayName(trimmed, "dead-letter") ??
    trimmed;
}

export function queueStreamDisplayName(name: string): string {
  return redisStreamDisplayName(name);
}

export interface QueueDepthRow {
  name: string;
  depth: number;
}

function numericValue(value: unknown): number | null {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const parsed = Number(trimmed);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function normalizeDepth(value: unknown): number {
  const parsed = numericValue(value);
  return parsed !== null && parsed > 0 ? Math.floor(parsed) : 0;
}

export function normalizeQueueDepths(
  depths: Record<string, unknown>,
): QueueDepthRow[] {
  const depthsByName = new Map<string, number>();
  for (const [rawName, rawDepth] of Object.entries(depths || {})) {
    const name = rawName.trim();
    if (!name || !isTaskStreamName(name)) continue;
    const depth = normalizeDepth(rawDepth);
    depthsByName.set(name, Math.max(depthsByName.get(name) || 0, depth));
  }

  return [...depthsByName.entries()]
    .map(([name, depth]) => ({ name, depth }))
    .sort((a, b) => b.depth - a.depth || a.name.localeCompare(b.name));
}
