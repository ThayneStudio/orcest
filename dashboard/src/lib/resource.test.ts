import { describe, expect, it } from "vitest";
import type { LockInfo, StuckTask } from "./types";
import {
  lockMatchesStuck,
  resourceLabel,
  resourceKey,
  resourceTypeLabel,
  stuckTaskAlertKey,
  stuckTaskKeys,
  stuckTaskResourceLabel,
} from "./resource";

function lock(resourceType: string, repo: string, resourceId: string): LockInfo {
  return {
    lock_key: `orcest:lock:${resourceType}:${repo}:${resourceId}`,
    prefix: "orcest",
    resource: `${repo}:${resourceId}`,
    resource_type: resourceType,
    repo,
    resource_id: resourceId,
    owner: "worker-1",
    ttl: 180,
    task_id: null,
    pending_created_at: null,
  };
}

function stuck(
  resourceType: string,
  repo: string | null,
  resourceId: string,
  prefix: string | null = "orcest",
): StuckTask {
  return {
    prefix,
    resource_type: resourceType,
    repo,
    resource_id: resourceId,
    reason: "stuck",
    severity: "warning",
  };
}

describe("resourceKey", () => {
  it("includes resource type, repo, and id", () => {
    expect(resourceKey("pr", "owner/repo", "42")).toBe(":pr:owner/repo:42");
  });

  it("includes namespace prefix when present", () => {
    expect(resourceKey("pr", "owner/repo", "42", "project-a"))
      .toBe("project-a:pr:owner/repo:42");
  });
});

describe("resource display labels", () => {
  it("formats known resource types and falls back for unknown types", () => {
    expect(resourceTypeLabel("pr")).toBe("PR");
    expect(resourceTypeLabel(" ISSUE ")).toBe("Issue");
    expect(resourceTypeLabel("")).toBe("?");
    expect(resourceTypeLabel("deployment")).toBe("deployment");
  });

  it("formats resource labels with optional prefix and repo", () => {
    expect(resourceLabel({
      prefix: "project-a",
      repo: "owner/repo",
      resource_type: "pr",
      resource_id: "42",
    })).toBe("[project-a] PR owner/repo #42");
    expect(resourceLabel({
      repo: "owner/repo",
      resource_type: "issue",
      resource_id: "7",
    }, { includeRepo: false })).toBe("Issue #7");
    expect(resourceLabel({
      repo: null,
      resource_type: "",
      resource_id: "",
    })).toBe("? #?");
  });
});

describe("lockMatchesStuck", () => {
  it("matches active locks by resource type, repo, and id", () => {
    const keys = stuckTaskKeys([stuck("pr", "owner/repo", "42")]);

    expect(lockMatchesStuck(lock("pr", "owner/repo", "42"), keys)).toBe(true);
    expect(lockMatchesStuck(lock("issue", "owner/repo", "42"), keys)).toBe(false);
    expect(lockMatchesStuck(lock("pr", "owner/repo", "43"), keys)).toBe(false);
    expect(lockMatchesStuck(lock("pr", "owner/other", "42"), keys)).toBe(false);
  });

  it("does not match a different namespace prefix", () => {
    const keys = stuckTaskKeys([stuck("pr", "owner/repo", "42", "project-a")]);

    expect(lockMatchesStuck(lock("pr", "owner/repo", "42"), keys)).toBe(false);
  });

  it("keeps legacy unprefixed stuck tasks matching prefixed locks", () => {
    const keys = stuckTaskKeys([stuck("pr", "owner/repo", "42", null)]);

    expect(lockMatchesStuck(lock("pr", "owner/repo", "42"), keys)).toBe(true);
  });

  it("matches repo-less stuck tasks to active locks with the same type and id", () => {
    const keys = stuckTaskKeys([stuck("pr", null, "42")]);

    expect(lockMatchesStuck(lock("pr", "owner/repo", "42"), keys)).toBe(true);
    expect(lockMatchesStuck(lock("pr", "owner/repo", "43"), keys)).toBe(false);
    expect(lockMatchesStuck(lock("issue", "owner/repo", "42"), keys)).toBe(false);
  });

  it("keeps repo-less stuck tasks namespace-aware when a prefix is present", () => {
    const keys = stuckTaskKeys([stuck("pr", null, "42", "project-a")]);

    expect(lockMatchesStuck(lock("pr", "owner/repo", "42"), keys)).toBe(false);
  });
});

describe("stuckTaskAlertKey", () => {
  it("distinguishes multiple alert reasons for the same resource", () => {
    const first = stuck("pr", "owner/repo", "42");
    const second = { ...first, reason: "attempts exhausted", severity: "critical" as const };

    expect(stuckTaskAlertKey(first, 0)).not.toBe(stuckTaskAlertKey(second, 0));
  });

  it("disambiguates duplicate alert records", () => {
    const task = stuck("pr", "owner/repo", "42");

    expect(stuckTaskAlertKey(task, 0)).not.toBe(stuckTaskAlertKey(task, 1));
  });
});

describe("stuckTaskResourceLabel", () => {
  it("formats repo resources with type and prefix", () => {
    expect(stuckTaskResourceLabel(stuck("pr", "owner/repo", "42", "project-a")))
      .toBe("[project-a] PR owner/repo #42");
    expect(stuckTaskResourceLabel(stuck("issue", "owner/repo", "7", null)))
      .toBe("Issue owner/repo #7");
  });

  it("formats consumer group stream alerts without a fake issue number", () => {
    expect(stuckTaskResourceLabel(stuck("stream", null, "tasks:claude/workers", null)))
      .toBe("stream tasks:claude group workers");
    expect(stuckTaskResourceLabel(stuck("stream", null, "tasks:grok", "project-a")))
      .toBe("[project-a] stream tasks:grok");
  });
});
