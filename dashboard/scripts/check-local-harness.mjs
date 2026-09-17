// Exercise the harness through the real authenticated dashboard and controls.
import assert from 'node:assert/strict';
import {spawn, spawnSync} from 'node:child_process';
import {mkdtemp, readFile, rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {WebSocket} from 'ws';

const state = await mkdtemp(join(tmpdir(), 'orcest-harness-check-'));
const python = process.env.ORCEST_TEST_PYTHON || '../.venv/bin/python';
const script = 'scripts/local-harness.py';
const child = spawn(python, [script, '--state-dir', state, '--port', '0', '--paused'], {
  stdio: ['ignore', 'pipe', 'pipe'], detached: true,
  env: {...process.env, REDIS_HOST: 'must-not-be-used.invalid', DASHBOARD_TOKEN: 'must-not-be-used'},
});
let diagnostics = '';
let childFailure;
let socketFailure;
let interrupted = false;
child.on('error', error => { childFailure = error; });
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => { interrupted = true; });
child.stdout.on('data', b => diagnostics += b);
child.stderr.on('data', b => diagnostics += b);
const until = async (check, description) => {
  const deadline = Date.now()+20000;
  while (Date.now()<deadline) {
    if (interrupted) throw new Error('Harness check interrupted');
    if (childFailure) throw childFailure;
    if (socketFailure) throw socketFailure;
    if (child.exitCode !== null || child.signalCode !== null) throw new Error(`Harness exited: ${diagnostics}`);
    try { const result = await check(); if (result) return result; } catch {}
    await new Promise(r => setTimeout(r, 200));
  }
  throw new Error(`Timed out: ${description}\n${diagnostics}`);
};
const status = async () => JSON.parse(await readFile(join(state, 'status.json'), 'utf8'));
let socket;
try {
  const initial = await until(async () => { const s=await status(); return s.url && s; }, 'startup');
  const base = initial.url;
  let cookie;
  const request = (path, options={}) => fetch(base+path, {
    ...options, headers: {...(cookie ? {Cookie:cookie} : {}), ...options.headers},
    signal: AbortSignal.timeout(10000),
  });
  const login = async () => {
    const response=await request('/api/auth/login', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({token:'local-harness'})});
    assert.equal(response.status,200);
    cookie=response.headers.get('set-cookie').split(';')[0];
  };
  const read = async () => { const r=await request('/api/work'); assert.equal(r.status,200); return r.json(); };
  const command = async name => {
    const result=spawnSync(python,[script,'--state-dir',state,'--command',name], {encoding:'utf8',timeout:10000});
    assert.equal(result.status,0,result.stderr);
    await new Promise(r=>setTimeout(r,300));
  };
  await until(async () => (await request('/api/ready')).status===200,'readiness');
  assert.equal((await request('/api/work')).status,401);
  await login();
  const seeded=await read();
  assert.equal(seeded.environment,'local-harness');
  assert.equal(seeded.projects.length,22);
  assert.equal(seeded.accounts.length,3);
  assert(seeded.accounts.some(a=>a.availability==='cooldown'));
  assert.equal(seeded.counts.needsHuman,1);
  assert.equal(seeded.workers.length,1);
  assert(!JSON.stringify(seeded).includes('FAKE_'));
  const work=(v,n)=>v.items.find(i=>i.project==='demo/orcest' && i.number===n);
  assert.equal(work(seeded,11).stage,'upcoming');
  assert.equal(work(seeded,11).activity,'waiting');
  assert.equal(work(seeded,10).activity,'queued');
  await command('step');
  const executing=await until(async()=>{const d=await read();return work(d,10)?.activity==='executing' && d;},'execution');
  const attempt=work(executing,10).latestAttempt;
  assert(attempt);
  const url=new URL('/ws/task-output',base);
  url.protocol='ws:';
  url.searchParams.set('worker_id',attempt.workerId);
  url.searchParams.set('task_id',attempt.taskId);
  url.searchParams.set('prefix',attempt.outputPrefix);
  socket=new WebSocket(url,{headers:{Cookie:cookie}});
  let messages=[];
  socket.on('message',data=>{
    try { messages.push(JSON.parse(data.toString())); } catch (error) { socketFailure = error; }
  });
  socket.on('error',error=>{ socketFailure = error; });
  await command('resume');
  await until(()=>messages.some(m=>JSON.stringify(m).includes('[simulation')),'live output');
  await command('pause');
  socket.close();
  await command('step');
  await until(async()=>{const d=await read();return work(d,10)?.stage==='in_progress' && work(d,10)?.reason==='Waiting for CI';},'CI wait');
  await command('step');
  await until(async()=>{const d=await read();return work(d,10)?.stage==='done' && work(d,11)?.activity==='queued';},'completion releases dependency');
  await command('step');
  await until(async()=>{const d=await read();return d.accounts.every(a=>a.availability==='available') && d.items.some(i=>i.project==='demo/transit' && i.number===30 && i.activity==='queued');},'provider cooldown release');
  await command('redis-down');
  await until(async()=>(await request('/api/work')).status===503,'Redis outage surfaced');
  await command('redis-up');
  await until(async()=>work(await read(),10)?.stage==='done','Redis recovery retains state');
  const previousPid=(await status()).dashboardPid;
  await command('restart-dashboard');
  await until(async()=>(await status()).dashboardPid!==previousPid,'dashboard process replacement');
  await until(async()=>(await request('/api/ready')).status===200,'dashboard restart');
  assert.equal((await request('/api/work')).status,401);
  await login();
  await command('reset');
  await until(async()=>work(await read(),10)?.activity==='queued','reset');
  assert.equal((await request('/api/auth/logout',{method:'POST'})).status,200);
  assert.equal((await request('/api/work')).status,401);
  console.log('Local harness passed: isolation, 22 projects, accounts/workers, dependency lifecycle, live output, Redis outage/recovery, session restart, reset and logout.');
} finally {
  socket?.terminate();
  try {
    if (child.pid && child.exitCode === null && child.signalCode === null) {
      child.kill('SIGTERM');
      await new Promise(resolve => {
        const timer = setTimeout(resolve, 30000);
        child.once('exit', () => { clearTimeout(timer); resolve(); });
      });
      if (child.exitCode === null && child.signalCode === null) {
        try { process.kill(-child.pid, 'SIGKILL'); } catch (error) { if (error.code !== 'ESRCH') throw error; }
        throw new Error('Harness cleanup timed out');
      }
    }
  } finally {
    await rm(state,{recursive:true,force:true});
  }
  if (interrupted) process.exitCode = 130;
}
