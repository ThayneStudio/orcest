#!/usr/bin/env node
// Fails if any TypeScript source under server/ or src/ is not covered by a
// type-check project.
//
// This exists because the failure is silent and was shipped once already: a
// child tsconfig that `extends` a parent INHERITS the parent's `exclude` (only
// `files`/`include` are overridden), so tsconfig.server.test.json initially
// resolved to zero test files while still exiting 0. `npm test` (vitest) strips
// types via esbuild and checks nothing, so nothing else would have caught it.
import { execFileSync } from "node:child_process";
import { readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";

const root = resolve(import.meta.dirname, "..");
const PROJECTS = ["tsconfig.json", "tsconfig.server.json", "tsconfig.server.test.json"];
const SOURCE_DIRS = ["server", "src"];

function walk(dir) {
  const out = [];
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    if (statSync(full).isDirectory()) out.push(...walk(full));
    else if (/\.tsx?$/.test(entry) && !entry.endsWith(".d.ts")) out.push(full);
  }
  return out;
}

const covered = new Set();
for (const project of PROJECTS) {
  const raw = execFileSync(
    join(root, "node_modules", ".bin", "tsc"),
    ["-p", project, "--showConfig"],
    { cwd: root, encoding: "utf8" },
  );
  for (const file of JSON.parse(raw).files ?? []) {
    covered.add(relative(root, resolve(root, file)));
  }
}

const missing = [];
for (const dir of SOURCE_DIRS) {
  for (const file of walk(join(root, dir))) {
    const rel = relative(root, file);
    if (!covered.has(rel)) missing.push(rel);
  }
}

if (missing.length > 0) {
  console.error(
    `${missing.length} TypeScript source(s) are not type-checked by any project ` +
      `(${PROJECTS.join(", ")}):\n` +
      missing.map((f) => `  ${f}`).join("\n") +
      `\n\nAdd them to a project's "include", or clear an inherited "exclude".`,
  );
  process.exit(1);
}

console.log(`Type-check coverage OK: ${covered.size} files across ${PROJECTS.length} projects.`);
