import { existsSync, readFileSync } from "node:fs";

const dashboardRoot = new URL("../", import.meta.url);
const repoRoot = new URL("../", dashboardRoot);
const minimumVersion = readText(new URL(".node-version", dashboardRoot)).trim();
const maximumMajor = parseVersion(minimumVersion)[0] + 1;

function readText(file) {
  return readFileSync(file, "utf8");
}

function parseVersion(version) {
  return version.split(".").map((part) => Number.parseInt(part, 10));
}

function compareVersions(left, right) {
  const leftParts = parseVersion(left);
  const rightParts = parseVersion(right);

  for (let i = 0; i < 3; i += 1) {
    const delta = leftParts[i] - rightParts[i];
    if (delta !== 0) return delta;
  }

  return 0;
}

function fail(message) {
  console.error(message);
  process.exit(1);
}

function ensureValidMinimumVersion() {
  if (!/^\d+\.\d+\.\d+$/.test(minimumVersion)) {
    fail(`dashboard/.node-version must contain an exact semver version, got "${minimumVersion}".`);
  }
}

function ensureConfigVersionsMatch() {
  const expectedEngine = `>=${minimumVersion} <${maximumMajor}`;
  const shellNodeVersionDefault = `\${DASHBOARD_NODE_VERSION:-${minimumVersion}}`;
  const checks = [
    {
      label: "dashboard/package.json engines.node",
      file: new URL("package.json", dashboardRoot),
      validate: (text) => JSON.parse(text).engines?.node === expectedEngine,
      expected: expectedEngine,
    },
    {
      label: "dashboard/Dockerfile NODE_VERSION default",
      file: new URL("Dockerfile", dashboardRoot),
      validate: (text) => text.includes(`ARG NODE_VERSION=${minimumVersion}`),
      expected: `ARG NODE_VERSION=${minimumVersion}`,
    },
    {
      label: "docker-compose.dashboard.yml DASHBOARD_NODE_VERSION default",
      file: new URL("docker-compose.dashboard.yml", repoRoot),
      validate: (text) => text.includes(`DASHBOARD_NODE_VERSION:-${minimumVersion}`),
      expected: `DASHBOARD_NODE_VERSION:-${minimumVersion}`,
    },
    {
      label: "dashboard/scripts/check-published.sh node image default",
      file: new URL("scripts/check-published.sh", dashboardRoot),
      validate: (text) => text.includes(`node:${minimumVersion}-slim`),
      expected: `node:${minimumVersion}-slim`,
    },
    {
      label: "dashboard/scripts/smoke-compose.sh node defaults",
      file: new URL("scripts/smoke-compose.sh", dashboardRoot),
      validate: (text) =>
        text.includes(`DASHBOARD_NODE_VERSION=${shellNodeVersionDefault}`) &&
        text.includes(`node:${shellNodeVersionDefault}-slim`),
      expected: `DASHBOARD_NODE_VERSION defaults to ${minimumVersion}`,
    },
  ];

  for (const check of checks) {
    if (!existsSync(check.file)) continue;
    const text = readText(check.file);
    if (!check.validate(text)) {
      fail(`${check.label} must match dashboard/.node-version (${check.expected}).`);
    }
  }
}

const currentVersion = process.versions.node;
const currentMajor = parseVersion(currentVersion)[0];

ensureValidMinimumVersion();
ensureConfigVersionsMatch();

if (compareVersions(currentVersion, minimumVersion) < 0 || currentMajor >= maximumMajor) {
  fail(
    `Dashboard requires Node >=${minimumVersion} <${maximumMajor}; current version is ${currentVersion}.`,
  );
}
