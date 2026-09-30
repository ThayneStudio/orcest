// Read-only fleet observation. Owns no fleet, dashboard, or tunnel processes.
// Run from dashboard/: node scripts/observe-live-fleet.mjs --url http://127.0.0.1:4320
//   --revision FULL_COMMIT_SHA --pid LOCAL_DASHBOARD_PID --state-dir NEW_PRIVATE_DIRECTORY --token-file PRIVATE_FILE
// Requires a pinned build/dist, authenticated gh CLI, and an already running local candidate.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { WebSocket } from 'ws';
import { qualifies, freshAttempt, verifiedDelivery, observeCoverage } from './live-evidence.mjs';

const args = process.argv.slice(2);
const option = (name, fallback) => {
  const index = args.indexOf(name);
  return index < 0 ? fallback : args[index + 1];
};
const base = new URL(option('--url'));
assert(base.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(base.hostname));
assert(!base.username && !base.password && !base.search && base.pathname === '/');
const revision = option('--revision');
assert(/^[0-9a-f]{40}$/.test(revision || ''), '--revision must be a full commit SHA');
const pid = Number(option('--pid'));
assert(Number.isInteger(pid) && pid > 1);
const directory = option('--state-dir');
assert(directory, '--state-dir is required');
const root = path.resolve(directory);
const seconds = Number(option('--seconds', 86400));
const cadence = Number(option('--sample-seconds', 60));
const maximumPartialSeconds = Number(option('--max-partial-seconds', 600));
assert(Number.isFinite(maximumPartialSeconds) && maximumPartialSeconds >= 60 && maximumPartialSeconds <= 600);
assert(Number.isFinite(seconds) && seconds >= 60);
assert(Number.isFinite(cadence) && cadence >= 5 && cadence <= 60);
let token = process.env.ORCEST_LIVE_TOKEN;
if (!token) {
  const file = option('--token-file');
  assert(file, '--token-file or ORCEST_LIVE_TOKEN is required');
  const metadata = fs.statSync(file);
  assert(metadata.isFile() && (metadata.mode & 0o077) === 0, 'Token file must be private');
  token = fs.readFileSync(file, 'utf8').trim();
}
assert(token && !token.includes('\n'), 'Expected a single dashboard token');
// Reject directory reuse entirely; evidence intervals are never resumed or stitched.
fs.mkdirSync(root, { mode: 0o700 });
const evidence = fs.openSync(path.join(root, 'samples.jsonl'), 'wx', 0o600);
function save(name, value) {
  const target = path.join(root, name);
  fs.writeFileSync(target + '.tmp', JSON.stringify(value, null, 2) + '\n', { mode: 0o600 });
  fs.renameSync(target + '.tmp', target);
}
const manifest = () => {
  const files = {};
  for (const directory of ['build', 'dist']) {
    for (const file of fs.readdirSync(directory, { recursive: true }).sort()) {
      const full = path.join(directory, file);
      if (fs.statSync(full).isFile()) files[full] = createHash('sha256').update(fs.readFileSync(full)).digest('hex');
    }
  }
  assert(Object.keys(files).length > 0, 'Build artifacts required');
  for (const full of ['scripts/observe-live-fleet.mjs','scripts/live-evidence.mjs'])
    files[full] = createHash('sha256').update(fs.readFileSync(full)).digest('hex');
  return files;
};
const processInfo = () => {
  const value = execFileSync('ps', ['-p', String(pid), '-o', 'lstart=', '-o', 'rss='],
    { encoding: 'utf8', timeout: 5000, stdio: ['ignore', 'pipe', 'ignore'] }).trim();
  const match = value.match(/^(.*?)\s+(\d+)$/);
  assert(match, 'Dashboard process unavailable');
  return { started: match[1], rssKiB: Number(match[2]) };
};
let interrupted = false;
let wake;
let socket;
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => {
  interrupted = true; wake?.(); socket?.terminate();
});
const sleep = ms => new Promise(resolve => {
  const timer = setTimeout(done, ms);
  function done() { clearTimeout(timer); wake = undefined; resolve(); }
  wake = done;
});
let cookie;
let loginAt;
const request = (route, options = {}) => fetch(new URL(route, base), {
  ...options, redirect: 'error', headers: { ...(cookie ? { Cookie: cookie } : {}), ...options.headers },
  signal: AbortSignal.timeout(15000),
});
async function login() {
  loginAt = Date.now();
  const response = await request('/api/auth/login', {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token }),
  });
  assert.equal(response.status, 200, 'Login failed');
  const header = response.headers.get('set-cookie');
  assert(header, 'Missing session cookie');
  cookie = header.split(';')[0];
}
async function inventory() {
  const items = [];
  let result;
  let offset = 0;
  let expectedTotal;
  const ids = new Set();
  const notices = new Set();
  let partial = false;
  while (offset !== null) {
    const response = await request(`/api/work?limit=500&offset=${offset}`);
    if (response.status === 401) {
      const age = (Date.now() - loginAt) / 1000;
      assert(age >= 43200 && age <= 43200 + cadence * 2 + 30, 'Unexpected session expiry');
      summary.sessionExpiryObserved = true;
      await login();
      continue;
    }
    assert.equal(response.status, 200, 'Work API failed');
    const page = await response.json();
    assert.equal(page.version, 1);
    assert(!page.environment, 'Synthetic feed rejected');
    summary.lastInventory = {coverage:page.coverage,total:page.total,notices:page.notices,staleItems:page.items.filter(item => item.stale).length};
    assert(Array.isArray(page.notices));
    assert(page.coverage !== 'unavailable', 'Fleet observations unavailable');
    assert(Array.isArray(page.items) && Number.isInteger(page.total) && page.total <= 5000);
    if (expectedTotal === undefined) expectedTotal = page.total;
    assert.equal(page.total, expectedTotal, 'Inventory changed during pagination');
    for (const item of page.items) {
      assert(typeof item.id === 'string' && !ids.has(item.id), 'Duplicate or invalid work ID');
      ids.add(item.id);
    }
    for (const notice of page.notices) notices.add(notice);
    if (page.coverage === 'partial') partial = true;
    items.push(...page.items);
    result = page;
    const next = page.nextOffset;
    assert(next === null || (Number.isInteger(next) && next > offset && next <= 5000), 'Invalid pagination');
    offset = next;
  }
  assert.equal(items.length, expectedTotal, 'Inventory pages missing records');
  const inventory = { ...result, items, notices:[...notices], coverage:partial ? 'partial' : 'complete' };
  summary.lastInventory = {coverage:inventory.coverage,total:inventory.total,notices:inventory.notices,
    staleItems:items.filter(item => item.stale).length};
  return inventory;
}
async function output(attempt, after) {
  const url = new URL('/ws/task-output', base);
  url.protocol = 'ws:';
  for (const [key, value] of Object.entries({ worker_id: attempt.workerId,
    task_id: attempt.taskId, prefix: attempt.outputPrefix, ...(after ? { after_id: after } : {}) }))
    url.searchParams.set(key, value);
  return new Promise((resolve, reject) => {
    const connection = new WebSocket(url, { headers: { Cookie: cookie }, handshakeTimeout: 10000 });
    socket = connection;
    let settled = false;
    const timer = setTimeout(() => finish(null, null), 10000);
    function finish(error, value) {
      if (settled) return;
      settled = true; clearTimeout(timer); connection.terminate();
      error ? reject(error) : resolve(value);
    }
    connection.on('message', raw => {
      try {
        const message = JSON.parse(raw.toString());
        assert(!message.error && !message.unavailable, 'Output unavailable');
        assert(Array.isArray(message.lines), 'Invalid output frame');
        if (message.lines.length) {
          assert(/^\d+-\d+$/.test(message.last_id), 'Invalid output cursor');
          if (after) {
            const old = after.split('-').map(BigInt), next = message.last_id.split('-').map(BigInt);
            assert(next[0] > old[0] || (next[0] === old[0] && next[1] > old[1]), 'Output cursor did not advance');
          }
          // Persist metadata only: agent text can include private data.
          finish(null, { cursor: message.last_id, lines: message.lines.length });
        } else if (message.done) finish(null, null);
      } catch { finish(new Error('Invalid or unavailable output frame')); }
    });
    connection.on('error', () => finish(new Error('Output connection failed')));
    connection.on('close', () => finish(new Error('Output closed without evidence')));
  });
}
const summary = { synthetic: false, status: 'starting', qualified: false, plannedSeconds: seconds,
  cadenceSeconds: cadence, maximumPartialSeconds,
  coverageHealth:{partialSince:null,completeSamples:0,partialSamples:0,recoveredEpisodes:0,maxPartialSeconds:0}, samples: 0, failures: [], outputAttempts: [], deliveries: [],
  sessionExpiryObserved: false, logoutVerified: false, elapsedSeconds: 0 };
let phase = 'initialization';
try {
  phase = 'initial artifact inventory';
  const artifacts = manifest();
  phase = 'initial readiness';
  const ready = await request('/api/ready');
  assert.equal(ready.status, 200);
  assert.equal((await ready.json()).revision, revision, 'Candidate revision mismatch');
  phase = 'initial process identity';
  const identity = processInfo();
  phase = 'unauthenticated API';
  assert.equal((await request('/api/work')).status, 401, 'Unauthenticated work API exposed');
  phase = 'initial login';
  await login();
  phase = 'initial inventory';
  const initial = await inventory();
  const expectedProjects = initial.projects.slice().sort();
  assert(expectedProjects.length > 0, 'Expected nonempty fleet project scope');
  const baseline = new Set(initial.items.map(item => item.latestAttempt?.taskId).filter(Boolean));
  const startedAt = Date.now();
  const monotonicStart = performance.now();
  summary.startedAt = new Date(startedAt).toISOString();
  summary.status = 'running';
  save('manifest.json', { artifacts, revision, pid, identity, url: base.origin,
    expectedProjects, baselineTaskIds: [...baseline], startedAt: summary.startedAt, collectorPid: process.pid });
  const cursors = new Map();
  const delivered = new Set();
  let previousWall = startedAt;
  let previousMonotonic = monotonicStart;
  let warmRss;
  while (!interrupted) {
    const wall = Date.now(), monotonic = performance.now();
    const elapsed = (monotonic - monotonicStart) / 1000;
    const sample = { at: new Date(wall).toISOString(), elapsedSeconds: elapsed };
    try {
      phase = 'sampling continuity';
      assert(Math.max(wall - previousWall, monotonic - previousMonotonic) <= Math.max(cadence * 1500, 30000), 'Sampling gap');
      previousWall = wall; previousMonotonic = monotonic;
      phase = 'process identity and memory';
      const process = processInfo();
      assert.equal(process.started, identity.started, 'Dashboard process replaced');
      assert(process.rssKiB > 0 && process.rssKiB < 512 * 1024, 'Dashboard memory budget exceeded');
      if (elapsed >= 900 && warmRss === undefined) warmRss = process.rssKiB;
      if (warmRss !== undefined) assert(process.rssKiB < Math.max(warmRss * 3, warmRss + 128 * 1024));
      sample.rssKiB = process.rssKiB;
      phase = 'readiness';
      const ready = await request('/api/ready');
      assert.equal(ready.status, 200);
      assert.equal((await ready.json()).revision, revision, 'Candidate revision changed');
      phase = 'authenticated inventory';
      const apiStarted = performance.now();
      const work = await inventory();
      sample.latencyMs = performance.now() - apiStarted;
      phase = 'source coverage and recovery';
      observeCoverage(summary.coverageHealth, work, elapsed, maximumPartialSeconds);
      assert.deepEqual(work.projects.slice().sort(), expectedProjects, 'Project scope changed');
      assert.equal(work.counts.upcoming + work.counts.in_progress + work.counts.done + work.counts.unknown, work.total, 'Work accounting inconsistent');
      sample.coverage = work.coverage;
      sample.notices = work.notices;
      sample.counts = work.counts;
      sample.projects = work.projects;
      sample.staleItems = work.items.filter(item => item.stale).length;
      sample.workers = work.workers.map(worker => ({id:worker.id,revision:worker.revision,ttl:worker.ttl}));
      sample.attempts = [];
      for (const item of work.items) {
        const attempt = item.latestAttempt;
        if (!freshAttempt(attempt, startedAt, baseline)) continue;
        sample.attempts.push({taskId:attempt.taskId,status:attempt.status,workId:item.id,startedAt:attempt.startedAt});
        if (attempt.status === 'running' && Date.now() - loginAt < 43180 * 1000) {
          phase = 'incremental output';
          const previous = cursors.get(attempt.taskId);
          const frame = await output(attempt, previous);
          if (frame) {
            cursors.set(attempt.taskId, frame.cursor);
            sample.output = [...(sample.output || []), {taskId:attempt.taskId,...frame}];
            if (previous && !summary.outputAttempts.includes(attempt.taskId)) summary.outputAttempts.push(attempt.taskId);
          }
        }
        if (!item.stale && item.stage === 'done' && summary.outputAttempts.includes(attempt.taskId) && !delivered.has(attempt.taskId)) {
          phase = 'independent GitHub delivery';
          const url = item.kind === 'pr' ? item.url : item.publicationUrl;
          if (!url) continue;
          assert(/^https:\/\/github\.com\/[^/]+\/[^/]+\/pull\/\d+$/.test(url), 'Invalid delivery URL');
          const pr = JSON.parse(execFileSync('gh', ['pr','view',url,'--json','url,state,mergedAt,mergeCommit,headRefOid'],
            {encoding:'utf8',timeout:15000,stdio:['ignore','pipe','ignore']}));
          assert(verifiedDelivery(item, pr, startedAt), 'Delivery was not independently verified');
          delivered.add(attempt.taskId);
          summary.deliveries.push({taskId:attempt.taskId,url:pr.url,mergedAt:pr.mergedAt,mergeCommit:pr.mergeCommit.oid,headRefOid:pr.headRefOid});
        }
      }
      phase = 'artifact identity';
      if (summary.samples % 10 === 0 || elapsed >= seconds) assert.deepEqual(manifest(), artifacts);
    } catch {
      // Never persist HTTP bodies, cookie values, output text, tokens, or gh stderr.
      sample.error = `Failed check: ${phase}`;
      summary.failures.push({at:sample.at,error:sample.error});
    }
    fs.writeSync(evidence, JSON.stringify(sample) + '\n'); fs.fsyncSync(evidence);
    summary.samples++; summary.elapsedSeconds = elapsed; summary.lastSampleAt = sample.at;
    save('summary.json', summary);
    if (summary.failures.length || elapsed >= seconds) break;
    await sleep(Math.max(0, Math.min(cadence * 1000 - (performance.now() - monotonic), seconds * 1000 - (performance.now() - monotonicStart))));
  }
  summary.status = interrupted ? 'interrupted' : summary.failures.length ? 'failed' : 'finished';
  if (summary.status === 'finished') {
    phase = 'session logout';
    assert.equal((await request('/api/auth/logout', {method:'POST'})).status, 200);
    assert.equal((await request('/api/work')).status, 401);
    summary.logoutVerified = true;
  }
} catch {
  summary.status = interrupted ? 'interrupted' : 'failed';
  if (!interrupted) summary.failures.push({at:new Date().toISOString(),error:`Failed check: ${phase}`});
} finally {
  socket?.terminate();
  if (interrupted) summary.status = 'interrupted';
  summary.qualified = qualifies(summary);
  summary.finishedAt = new Date().toISOString();
  save('summary.json', summary);
  fs.closeSync(evidence);
  console.log(JSON.stringify(summary, null, 2));
  process.exitCode = summary.status === 'interrupted' ? 130 : summary.status === 'failed' ? 1 : 0;
}
