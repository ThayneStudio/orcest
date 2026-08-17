import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export function resolveDashboardDistPath(
  cwd = process.cwd(),
  moduleDir = path.dirname(fileURLToPath(import.meta.url)),
): string {
  const candidates = [
    path.resolve(cwd, "dist"),
    path.resolve(cwd, "dashboard/dist"),
    path.resolve(moduleDir, "../dist"),
    path.resolve(moduleDir, "../../dist"),
  ];

  for (const candidate of candidates) {
    if (fs.existsSync(path.join(candidate, "index.html"))) {
      return candidate;
    }
  }

  return candidates[0];
}
