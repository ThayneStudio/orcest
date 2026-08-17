import type { LockInfo, StuckTask } from "./types";

type ResourceLike = {
  prefix?: string | null;
  repo?: string | null;
  resource_type: string;
  resource_id: string;
};

type ResourceLabelOptions = {
  includePrefix?: boolean;
  includeRepo?: boolean;
};

export function resourceKey(
  resourceType: string,
  repo: string | null,
  resourceId: string,
  prefix: string | null = null,
): string {
  return `${prefix || ""}:${resourceType}:${repo || ""}:${resourceId}`;
}

export function stuckTaskKeys(stuckTasks: StuckTask[]): Set<string> {
  return new Set(
    stuckTasks.map((task) =>
      resourceKey(task.resource_type, task.repo, task.resource_id, task.prefix)
    ),
  );
}

export function stuckTaskAlertKey(task: StuckTask, occurrence: number): string {
  return JSON.stringify([
    task.prefix || "",
    task.resource_type,
    task.repo || "",
    task.resource_id,
    task.severity,
    task.reason,
    occurrence,
  ]);
}

export function resourceTypeLabel(type: string): string {
  switch (type.trim().toLowerCase()) {
    case "pr":
      return "PR";
    case "issue":
      return "Issue";
    default:
      return type.trim() || "?";
  }
}

export function resourceLabel(
  resource: ResourceLike,
  options: ResourceLabelOptions = {},
): string {
  const includePrefix = options.includePrefix ?? true;
  const includeRepo = options.includeRepo ?? true;
  const prefix = includePrefix && resource.prefix ? `[${resource.prefix}] ` : "";
  const repo = resource.repo?.trim();
  const resourceId = resource.resource_id.trim() || "?";
  return `${prefix}${resourceTypeLabel(resource.resource_type)}${includeRepo && repo ? ` ${repo}` : ""} #${resourceId}`;
}

export function stuckTaskResourceLabel(task: StuckTask): string {
  const prefix = task.prefix ? `[${task.prefix}] ` : "";
  if (task.resource_type === "stream") {
    const [stream, group] = task.resource_id.split("/", 2);
    return `${prefix}stream ${stream}${group ? ` group ${group}` : ""}`;
  }

  return resourceLabel(task);
}

export function lockMatchesStuck(lock: LockInfo, stuckKeys: Set<string>): boolean {
  const candidates = [
    resourceKey(lock.resource_type, lock.repo, lock.resource_id, lock.prefix),
    resourceKey(lock.resource_type, lock.repo, lock.resource_id),
    resourceKey(lock.resource_type, null, lock.resource_id, lock.prefix),
    resourceKey(lock.resource_type, null, lock.resource_id),
  ];
  return candidates.some((key) => stuckKeys.has(key));
}
