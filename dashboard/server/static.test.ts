import fs from "fs";
import os from "os";
import path from "path";
import { afterEach, describe, expect, it } from "vitest";
import { resolveDashboardDistPath } from "./static.js";

const tempRoots = new Set<string>();

function tempRoot(): string {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "orcest-dashboard-static-"));
  tempRoots.add(root);
  return root;
}

function writeIndex(dir: string) {
  fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(path.join(dir, "index.html"), "<!doctype html>", "utf-8");
}

afterEach(() => {
  for (const root of tempRoots) {
    fs.rmSync(root, { recursive: true, force: true });
  }
  tempRoots.clear();
});

describe("resolveDashboardDistPath", () => {
  it("uses cwd/dist in the built Docker/package layout", () => {
    const cwd = tempRoot();
    writeIndex(path.join(cwd, "dist"));

    expect(resolveDashboardDistPath(cwd, path.join(cwd, "build/server"))).toBe(
      path.join(cwd, "dist"),
    );
  });

  it("uses dashboard/dist when launched from the repository root", () => {
    const cwd = tempRoot();
    writeIndex(path.join(cwd, "dashboard/dist"));

    expect(resolveDashboardDistPath(cwd, path.join(cwd, "dashboard/build/server"))).toBe(
      path.join(cwd, "dashboard/dist"),
    );
  });

  it("uses source and compiled module-relative layouts", () => {
    const cwd = tempRoot();
    const dashboardRoot = path.join(cwd, "dashboard");
    writeIndex(path.join(dashboardRoot, "dist"));

    expect(resolveDashboardDistPath(
      path.join(cwd, "other"),
      path.join(dashboardRoot, "server"),
    )).toBe(path.join(dashboardRoot, "dist"));
    expect(resolveDashboardDistPath(
      path.join(cwd, "other"),
      path.join(dashboardRoot, "build/server"),
    )).toBe(path.join(dashboardRoot, "dist"));
  });
});
