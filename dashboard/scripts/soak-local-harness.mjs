// Sustained synthetic evidence. Never counts as a live-fleet observation.
import assert from 'node:assert/strict';
import { spawn, spawnSync, execFileSync } from 'node:child_process';
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { WebSocket } from 'ws';

const args = process.argv.slice(2);
function option(name, fallback) {
  const index = args.indexOf(name);
  return index < 0 ? fallback : args[index + 1];
}
const directory = option('--state-dir');
if (!directory) throw new Error('--state-dir must name a new persistent evidence directory');
const root = path.resolve(directory);
const seconds = Number(option('--seconds', 13 * 3600));
const cadence = Number(option('--sample-seconds', 60));
assert(Number.isFinite(seconds) && seconds >= 60);
assert(Number.isFinite(cadence) && cadence >= 5 && cadence <= 60);
fs.mkdirSync(root, { recursive: true, mode: 0o700 });
// Exclusive creation prevents overwriting or stitching together observation intervals.
const evidence = fs.openSync(path.join(root, 'samples.jsonl'), 'wx', 0o600);
const logs = fs.openSync(path.join(root, 'harness.log'), 'a', 0o600);
const python = process.env.ORCEST_TEST_PYTHON || '../.venv/bin/python';
const harnessDir = path.join(root, 'fleet');
const processStarted = Date.now();
let child;
let childFailure = null;
const sha = file => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const manifest = {};
function save(name, value) {
  const target = path.join(root, name);
  fs.writeFileSync(target + '.tmp', JSON.stringify(value, null, 2) + '\n', { mode: 0o600 });
  fs.renameSync(target + '.tmp', target);
}
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const readStatus = () => JSON.parse(fs.readFileSync(path.join(harnessDir, 'status.json'), 'utf8'));
async function eventually(check, label, timeout = 30000) {
  const until = Date.now() + timeout;
  while (Date.now() < until) {
    if (interrupted) throw new Error('Observation interrupted');
    if (childFailure) throw childFailure;
    if (child.exitCode !== null || child.signalCode !== null) throw new Error(`Harness exited (${child.exitCode}): ${label}`);
    try { const value = await check(); if (value) return value; } catch {}
    await sleep(200);
  }
  throw new Error(`Timed out: ${label}`);
}
function command(name) {
  const result = spawnSync(python, ['scripts/local-harness.py', '--state-dir', harnessDir, '--command', name], { encoding: 'utf8', timeout: 10000 });
  assert.equal(result.status, 0, result.stderr);
}
const started = Date.now();
let running = true;
let interrupted = false;
let wakeSample;
for (const sig of ['SIGINT', 'SIGTERM']) process.on(sig, () => {
  interrupted = true; running = false; wakeSample?.();
});
const sampleSleep = ms => new Promise(resolve => {
  const timer = setTimeout(finish, ms);
  function finish() { clearTimeout(timer); wakeSample = undefined; resolve(); }
  wakeSample = finish;
});
const summary = {
  synthetic: true, status: 'starting', qualified: false, startedAt: new Date(started).toISOString(),
  plannedSeconds: seconds, cadenceSeconds: cadence, samples: 0, failures: [],
  distinctAttempts: 0, completedSamples: 0, reconnectChecks: 0,
  staleObserved: false, freshnessRecovered: false, sessionExpiryObserved: false,
};
let cookie;
let loginAt;
let sessionExpiryAfterSeconds;
let socket;
try {
  for (const dir of ['build', 'dist']) {
    for (const file of fs.readdirSync(dir, { recursive: true })) {
      const full = path.join(dir, file);
      if (fs.statSync(full).isFile()) manifest[full] = sha(full);
    }
  }
  child = spawn(python, ['scripts/local-harness.py', '--state-dir', harnessDir, '--port', '0'], {
    stdio: ['ignore', logs, logs], detached: true,
  });
  child.on('error', error => { childFailure = error; });
  const initial = await eventually(() => { const s = readStatus(); return s.url && s; }, 'harness startup');
  const base = initial.url;
  const identity = { supervisor: initial.pid, redis: initial.redisPid, dashboard: initial.dashboardPid };
  save('manifest.json', { files: manifest, identity, url: base, collectorPid: process.pid,
    createdAt: new Date().toISOString(), processStartedAt: new Date(processStarted).toISOString() });
  const request = (route, options = {}) => fetch(base + route, {
    ...options, headers: { ...(cookie ? { Cookie: cookie } : {}), ...options.headers }, signal: AbortSignal.timeout(15000),
  });
  async function login() {
    const requestedAt = Date.now();
    const response = await request('/api/auth/login', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{"token":"local-harness"}' });
    assert.equal(response.status, 200);
    cookie = response.headers.get('set-cookie').split(';')[0];
    loginAt = requestedAt;
  }
  await eventually(async () => (await request('/api/ready')).status === 200, 'readiness');
  await login();
  const firstLoginAt = loginAt;
  summary.status = 'running';
  const observationStarted = performance.now();
  summary.startedAt = new Date().toISOString();
  const attempts = new Set();
  const deliveries = new Set();
  let previousSample = Date.now();
  let previousSampleMonotonic = observationStarted;
  let sourceStopped = false;
  let sourceResumed = false;
  let warmRss = null;
  let maxRss = 0;
  let lastRss = 0;
  async function outputFrame(attempt, after, priorLines = []) {
    const url = new URL('/ws/task-output', base);
    url.protocol = 'ws:';
    url.searchParams.set('worker_id', attempt.workerId);
    url.searchParams.set('task_id', attempt.taskId);
    url.searchParams.set('prefix', attempt.outputPrefix);
    if (after) url.searchParams.set('after_id', after);
    return new Promise((resolve, reject) => {
      const connection = new WebSocket(url, { headers: { Cookie: cookie } });
      socket = connection;
      let settled = false;
      const timeout = setTimeout(() => finish(new Error('Output probe timed out')), 10000);
      const finish = (error, result) => { if (settled) return; settled = true; clearTimeout(timeout); connection.terminate(); error ? reject(error) : resolve(result); };
      connection.on('message', raw => {
        try {
          const message = JSON.parse(raw.toString());
          if (message.error || message.unavailable) return finish(new Error('Output became unavailable'));
          assert(Array.isArray(message.lines), 'Output frame has no lines array');
          assert(typeof message.last_id === 'string' && /^\d+-\d+$/.test(message.last_id), 'Output frame has no valid cursor');
          if (message.lines.length) {
            if (after) {
              const [oldTime, oldSequence] = after.split('-').map(BigInt);
              const [newTime, newSequence] = message.last_id.split('-').map(BigInt);
              assert(newTime > oldTime || (newTime === oldTime && newSequence > oldSequence), 'Output cursor did not advance');
            }
            if (message.lines.some(line => priorLines.includes(line))) return finish(new Error('Output cursor replayed a previously read line'));
            return finish(null, message);
          }
          if (message.done) finish(new Error('Execution ended during output probe'));
        } catch (error) { finish(error); }
      });
      connection.on('error', error => finish(error));
      connection.on('close', () => finish(new Error('Output probe closed before delivering lines')));
    });
  }
  while (running) {
    const sampleStart = Date.now();
    const sampleMonotonic = performance.now();
    const elapsed = (sampleMonotonic - observationStarted) / 1000;
    const sample = { at: new Date(sampleStart).toISOString(), elapsedSeconds: elapsed };
    try {
      assert(Math.max(sampleStart - previousSample, sampleMonotonic - previousSampleMonotonic) <= Math.max(cadence * 1500, 30000), 'Sampling gap exceeds allowance');
      if (childFailure) throw childFailure;
      assert(child.exitCode === null && child.signalCode === null, 'Harness exited during observation');
      previousSample = sampleStart;
      previousSampleMonotonic = sampleMonotonic;
      const status = readStatus();
      assert.equal(status.pid, identity.supervisor);
      assert.equal(status.redisPid, identity.redis);
      assert.equal(status.dashboardPid, identity.dashboard);
      assert(Date.now() / 1000 - status.updatedAt < 10, 'Harness heartbeat is stale');
      if (elapsed >= 300 && !sourceStopped) {
        command('source-down'); sourceStopped = true;
      }
      if (elapsed >= 600 && !sourceResumed) {
        assert(summary.staleObserved, 'Source outage did not surface stale evidence');
        command('source-up'); sourceResumed = true;
      }
      assert.equal((await request('/api/ready')).status, 200);
      const apiStart = performance.now();
      let response = await request('/api/work');
      if (response.status === 401) {
        const age = (Date.now() - loginAt) / 1000;
        assert(age >= 43200 && age <= 43200 + cadence * 2, 'Unexpected session expiry');
        summary.sessionExpiryObserved = true;
        sessionExpiryAfterSeconds = (Date.now() - firstLoginAt) / 1000;
        await login();
        response = await request('/api/work');
      }
      assert.equal(response.status, 200);
      const work = await response.json();
      assert.equal(work.environment, 'local-harness');
      assert.equal(work.projects.length, 22);
      assert(!JSON.stringify(work).includes('FAKE_'), 'Fixture credentials leaked');
      sample.latencyMs = performance.now() - apiStart;
      sample.counts = work.counts;
      sample.phase = status.phase;
      sample.stale = work.items.some(item => item.stale);
      if (sourceStopped && !sourceResumed && sample.stale) summary.staleObserved = true;
      if (sourceResumed && !sample.stale && work.coverage === 'complete') summary.freshnessRecovered = true;
      for (const item of work.items) if (item.latestAttempt) attempts.add(item.latestAttempt.taskId);
      if (work.counts.done > 0) summary.completedSamples++;
      for (const item of work.items) if (item.stage === "done" && item.latestAttempt) deliveries.add(item.latestAttempt.taskId);
      const selected = work.items.find(item => item.project === 'demo/sparkmaw' && item.number === 20 && item.latestAttempt?.status === 'running');
      // Phase 4 is about to reset the fixture. Probe only when an execution has
      // at least one full stage left, so resets are not mistaken for outages.
      if (selected && status.phase < 4 && Date.now() - loginAt < 43180 * 1000) {
        const first = await outputFrame(selected.latestAttempt);
        const next = await outputFrame(selected.latestAttempt, first.last_id, first.lines);
        assert(next.lines.length > 0);
        summary.reconnectChecks++;
      }
      lastRss = Number(execFileSync('ps', ['-p', String(identity.dashboard), '-o', 'rss='], { encoding: 'utf8' }).trim());
      assert(Number.isFinite(lastRss) && lastRss > 0);
      maxRss = Math.max(maxRss, lastRss);
      if (elapsed >= 900 && warmRss === null) warmRss = lastRss;
      assert(lastRss < 512 * 1024, 'Dashboard RSS exceeds 512 MiB');
      if (warmRss !== null) assert(lastRss < Math.max(warmRss * 3, warmRss + 128 * 1024), 'Sustained memory growth exceeds the observation budget');
      sample.rssKiB = lastRss;
      if (summary.samples % 10 === 0 || elapsed >= seconds) for (const [file, expected] of Object.entries(manifest)) assert.equal(sha(file), expected, `Artifact changed: ${file}`);
    } catch (error) {
      sample.error = String(error.message || error);
      summary.failures.push({ at: sample.at, error: sample.error });
      running = false;
    }
    fs.writeSync(evidence, JSON.stringify(sample) + '\n');
    fs.fsyncSync(evidence);
    summary.samples++;
    Object.assign(summary, { lastSampleAt: sample.at, elapsedSeconds: sample.elapsedSeconds,
      distinctAttempts: attempts.size, completedAttempts: deliveries.size, maxRssKiB: maxRss, lastRssKiB: lastRss,
      warmRssKiB: warmRss, sessionExpiryAfterSeconds });
    save('summary.json', summary);
    if (elapsed >= seconds) break;
    if (running) await sampleSleep(Math.max(0, Math.min(cadence * 1000 - (performance.now() - sampleMonotonic), seconds * 1000 - (performance.now() - observationStarted))));
  }
  summary.status = interrupted ? 'interrupted' : summary.failures.length ? 'failed' : 'finished';
  summary.qualified = summary.status === 'finished' && summary.elapsedSeconds >= seconds && summary.elapsedSeconds >= 13 * 3600 &&
    summary.sessionExpiryObserved && summary.staleObserved && summary.freshnessRecovered &&
    summary.distinctAttempts >= 10 && summary.completedAttempts >= 10 && summary.reconnectChecks >= 10;
} catch (error) {
  summary.status = interrupted ? 'interrupted' : 'failed';
  if (!interrupted) summary.failures.push({ at: new Date().toISOString(), error: String(error.message || error) });
} finally {
  socket?.terminate();
  if (child?.pid && child.exitCode === null && child.signalCode === null) {
    child.kill('SIGTERM');
    await new Promise(resolve => {
      const timer = setTimeout(resolve, 30000);
      child.once('exit', () => { clearTimeout(timer); resolve(); });
    });
    if (child.exitCode === null && child.signalCode === null) {
      // The harness owns its process group; kill only this invocation's descendants.
      try { process.kill(-child.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
      summary.failures.push({ error: 'Harness did not shut down within 30 seconds' });
      summary.status = 'failed'; summary.qualified = false;
    }
  }
  if (interrupted) { summary.status = 'interrupted'; summary.qualified = false; }
  summary.finishedAt = new Date().toISOString();
  save('summary.json', summary);
  fs.closeSync(evidence);
  fs.closeSync(logs);
  console.log(JSON.stringify(summary, null, 2));
  if (summary.status === 'failed') process.exitCode = 1;
  if (summary.status === 'interrupted') process.exitCode = 130;
}
