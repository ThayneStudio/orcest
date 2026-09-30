import test from 'node:test';
import assert from 'node:assert/strict';
import { qualifies, freshAttempt, verifiedDelivery, observeCoverage, createCoverageHealth } from './live-evidence.mjs';
const healthy = {status:'finished', synthetic:false, failures:[], elapsedSeconds:86400,
  cadenceSeconds:60,samples:1441, coverageHealth:{completeSamples:1400,lastCoverage:'complete',openEpisodes:{}},
  sessionExpiryObserved:true, logoutVerified:true, outputAttempts:['new'], deliveries:[{taskId:'new'}]};
test('only uninterrupted full live evidence with output and matching delivery qualifies', () => {
  assert(qualifies(healthy));
  for (const patch of [{status:'interrupted'}, {synthetic:true}, {failures:['gap']},
    {elapsedSeconds:86399}, {samples:2}, {cadenceSeconds:undefined}, {sessionExpiryObserved:false}, {logoutVerified:false},
    {coverageHealth:{completeSamples:0,lastCoverage:'complete',openEpisodes:{}}},
    {coverageHealth:{completeSamples:10,lastCoverage:'partial',openEpisodes:{}}},
    {coverageHealth:{completeSamples:10,lastCoverage:'complete',openEpisodes:{missing:{since:0}}}},
    {outputAttempts:[]}, {deliveries:[]}, {deliveries:[{taskId:'historical'}]}]) {
    assert.equal(qualifies({...healthy,...patch}), false, JSON.stringify(patch));
  }
});
test('baseline attempts and pre-interval execution never count', () => {
  const baseline = new Set(['old']);
  assert.equal(freshAttempt({taskId:'old',startedAt:2000},1000000,baseline), false);
  assert.equal(freshAttempt({taskId:'new',startedAt:999},1000000,baseline), false);
  assert.equal(freshAttempt({taskId:'new',startedAt:null},1000000,baseline), false);
  assert(freshAttempt({taskId:'new',startedAt:1001},1000000,baseline));
});
test('independent verification requires the observed full publication revision', () => {
  const work = {kind:'issue',publicationUrl:'https://github.com/a/b/pull/1',headSha:'a'.repeat(40)};
  const pr = {url:work.publicationUrl,state:'MERGED',mergedAt:'2026-09-30T01:00:00Z',mergeCommit:{oid:'b'.repeat(40)},headRefOid:work.headSha};
  const start = Date.parse('2026-09-30T00:00:00Z');
  assert(verifiedDelivery(work,pr,start));
  for (const patch of [{url:'https://github.com/a/b/pull/2'}, {state:'OPEN'}, {mergeCommit:null},
    {mergedAt:'2026-09-29T23:59:00Z'}, {headRefOid:'c'.repeat(40)}, {mergedAt:null}])
    assert.equal(verifiedDelivery(work,{...pr,...patch},start), false);
  for (const headSha of [undefined,null,'','abc']) assert.equal(verifiedDelivery({...work,headSha},pr,start),false);
});
const notice = 'Some source observations are stale. Last-known work is retained.';
const inventory = (sourceStale = false, items = []) => ({
  coverage:sourceStale || items.some(item => item.stale) ? 'partial' : 'complete',
  notices:sourceStale || items.some(item => item.stale) ? [notice] : [],
  sourceObservations:[{id:'source-a',project:'a/b',observedAt:100,stale:sourceStale}],items,
});
const observe = (health, value, elapsed) => observeCoverage(health,value,elapsed,600);
test('overlapping individually recovering work episodes do not fail on their union duration', () => {
  const health = createCoverageHealth(['source-a']);
  observe(health,inventory(false,[{id:'a',stale:true},{id:'b',stale:false}]),0);
  observe(health,inventory(false,[{id:'a',stale:true},{id:'b',stale:true}]),300);
  observe(health,inventory(false,[{id:'a',stale:false},{id:'b',stale:true}]),500);
  observe(health,inventory(false,[{id:'a',stale:false},{id:'b',stale:true}]),700);
  observe(health,inventory(false,[{id:'a',stale:false},{id:'b',stale:false}]),800);
  assert.equal(health.recoveredEpisodes,2);
  assert.equal(health.maxEpisodeSeconds,500);
  assert.equal(health.maxUnionPartialSeconds,800);
  assert.deepEqual(health.openEpisodes,{});
  assert.equal(health.completeSamples,1);
});
test('continuously stale work cannot reset its clock through timestamp changes or other recovery', () => {
  const health = createCoverageHealth(['source-a']);
  observe(health,inventory(false,[{id:'a',stale:true,observedAt:10},{id:'b',stale:true}]),0);
  observe(health,inventory(false,[{id:'a',stale:true,observedAt:100},{id:'b',stale:false}]),500);
  assert.throws(() => observe(health,inventory(false,[{id:'a',stale:true,observedAt:200}]),601));
});
test('project freshness remains attributable while unrelated stale work recovers', () => {
  const health = createCoverageHealth(['source-a']);
  observe(health,inventory(true,[{id:'a',stale:true}]),0);
  observe(health,inventory(true,[{id:'a',stale:false}]),500);
  assert.deepEqual(Object.keys(health.openEpisodes),['source:source-a']);
  assert.throws(() => observe(health,inventory(true,[{id:'a',stale:false}]),601));
});
test('disappearance proves neither project nor work recovery', () => {
  const health = createCoverageHealth(['source-a']);
  observe(health,inventory(true,[{id:'a',stale:true}]),0);
  observe(health,{...inventory(),sourceObservations:[]},300);
  assert.deepEqual(Object.keys(health.openEpisodes),['source:source-a','work:a']);
  assert.equal(health.completeSamples,0);
  assert.throws(() => observe(health,inventory(),601));
});
test('fresh recovery after the deadline fails; recovery at its boundary passes', () => {
  const late = createCoverageHealth(['source-a']);
  observe(late,inventory(true),0);
  assert.throws(() => observe(late,inventory(),601));
  const onTime = createCoverageHealth(['source-a']);
  observe(onTime,inventory(true),0);
  observe(onTime,inventory(),600);
  assert.equal(onTime.recoveredEpisodes,1);
});
test('unavailable, unknown, unattributed or missing metadata cannot count as coverage', () => {
  const health = createCoverageHealth(['source-a']);
  for (const value of [{...inventory(),coverage:'unavailable'},
    {...inventory(),coverage:'partial',notices:['Some operational data is unavailable.']},
    {...inventory(),coverage:'partial',notices:[notice]},
    {...inventory(),sourceObservations:undefined},
    {...inventory(),sourceObservations:[{id:'other',stale:false}]}]) assert.throws(() => observe(health,value,0));
});
