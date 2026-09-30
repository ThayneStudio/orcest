import test from 'node:test';
import assert from 'node:assert/strict';
import { qualifies, freshAttempt, verifiedDelivery, observeCoverage } from './live-evidence.mjs';
const healthy = {status:'finished', synthetic:false, failures:[], elapsedSeconds:86400,
  cadenceSeconds:60,samples:1441, coverageHealth:{completeSamples:1400,partialSince:null}, sessionExpiryObserved:true, logoutVerified:true, outputAttempts:['new'], deliveries:[{taskId:'new'}]};
test('only uninterrupted full live evidence with output and matching delivery qualifies', () => {
  assert(qualifies(healthy));
  for (const patch of [{status:'interrupted'}, {synthetic:true}, {failures:['gap']},
    {elapsedSeconds:86399}, {samples:2}, {cadenceSeconds:undefined}, {sessionExpiryObserved:false}, {logoutVerified:false},
    {coverageHealth:{completeSamples:0,partialSince:null}}, {coverageHealth:{completeSamples:10,partialSince:100}}, {outputAttempts:[]}, {deliveries:[]}, {deliveries:[{taskId:'historical'}]}]) {
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
test('independent verification rejects old, unrelated, unmerged and mismatched revisions', () => {
  const work = {kind:'issue',publicationUrl:'https://github.com/a/b/pull/1',headSha:'abc'};
  const pr = {url:work.publicationUrl,state:'MERGED',mergedAt:'2026-09-30T01:00:00Z',mergeCommit:{oid:'def'},headRefOid:'abc'};
  const start = Date.parse('2026-09-30T00:00:00Z');
  assert(verifiedDelivery(work,pr,start));
  for (const patch of [{url:'https://github.com/a/b/pull/2'}, {state:'OPEN'}, {mergeCommit:null},
    {mergedAt:'2026-09-29T23:59:00Z'}, {headRefOid:'wrong'}, {mergedAt:null}])
    assert.equal(verifiedDelivery(work,{...pr,...patch},start), false);
});

test('stale retained observations must recover within a bounded episode', () => {
  const health = {partialSince:null,completeSamples:0,partialSamples:0,recoveredEpisodes:0,maxPartialSeconds:0};
  const partial = {coverage:'partial',notices:['Some source observations are stale. Last-known work is retained.']};
  observeCoverage(health,partial,0,600);
  observeCoverage(health,partial,599,600);
  assert.throws(() => observeCoverage(health,partial,601,600));
  assert.throws(() => observeCoverage(health,{coverage:'complete',notices:[]},602,600));
  observeCoverage(health,{coverage:'complete',notices:[]},600,600);
  assert.equal(health.partialSince,null);
  assert.equal(health.recoveredEpisodes,1);
  assert.throws(() => observeCoverage(health,{coverage:'unavailable',notices:[]},603,600));
  assert.throws(() => observeCoverage(health,{coverage:'partial',notices:['Some operational data is unavailable.']},603,600));
});
