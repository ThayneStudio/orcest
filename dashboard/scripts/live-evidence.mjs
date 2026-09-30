// Pure qualification rules shared by the collector and its regression checks.
export function qualifies(summary) {
  return summary.status === 'finished' && !summary.synthetic && summary.failures.length === 0 &&
    summary.elapsedSeconds >= 86400 && Number.isFinite(summary.cadenceSeconds) &&
    summary.cadenceSeconds >= 5 && summary.cadenceSeconds <= 60 &&
    summary.samples >= Math.ceil(86400 / Math.max(summary.cadenceSeconds * 1.5, 30)) + 1 && summary.sessionExpiryObserved &&
    summary.logoutVerified && summary.coverageHealth?.completeSamples > 0 &&
    summary.coverageHealth.lastCoverage === 'complete' &&
    Object.keys(summary.coverageHealth.openEpisodes || {}).length === 0 && summary.outputAttempts.length > 0 &&
    summary.deliveries.some(delivery => summary.outputAttempts.includes(delivery.taskId));
}
export function freshAttempt(attempt, startedAt, baseline) {
  return !!attempt && !baseline.has(attempt.taskId) && Number.isFinite(attempt.startedAt) &&
    attempt.startedAt * 1000 >= startedAt;
}
export function verifiedDelivery(work, pr, startedAt) {
  const expected = work.kind === 'pr' ? work.url : work.publicationUrl;
  return !!expected && pr.url === expected && pr.state === 'MERGED' && !!pr.mergeCommit?.oid &&
    Number.isFinite(Date.parse(pr.mergedAt)) && Date.parse(pr.mergedAt) >= startedAt &&
    typeof work.headSha === 'string' && /^[0-9a-f]{40}$/.test(work.headSha) && work.headSha === pr.headRefOid;
}

const STALE_NOTICE = 'Some source observations are stale. Last-known work is retained.';
export function createCoverageHealth(expectedSourceIds) {
  return {expectedSourceIds:[...expectedSourceIds],openEpisodes:{},completeSamples:0,partialSamples:0,
    recoveredEpisodes:0,maxEpisodeSeconds:0,partialSince:null,maxUnionPartialSeconds:0,lastCoverage:null};
}
export function observeCoverage(health, inventory, elapsed, maximumPartialSeconds) {
  if (inventory.coverage === 'unavailable') throw new Error('Fleet observations unavailable');
  if (!['complete','partial'].includes(inventory.coverage) || !Array.isArray(inventory.notices) ||
    inventory.notices.some(notice => notice !== STALE_NOTICE) ||
    (inventory.coverage === 'partial' && !inventory.notices.length))
    throw new Error('Unexplained degraded coverage');
  if (!Array.isArray(inventory.sourceObservations)) throw new Error('Project freshness metadata missing');
  const records = new Map();
  const sourceIds = new Set();
  for (const source of inventory.sourceObservations) {
    if (typeof source.id !== 'string' || !source.id || sourceIds.has(source.id) ||
      typeof source.stale !== 'boolean' || !health.expectedSourceIds.includes(source.id))
      throw new Error('Invalid or changed source identity');
    sourceIds.add(source.id);
    records.set(`source:${source.id}`, source.stale);
  }
  for (const item of inventory.items) {
    if (typeof item.id !== 'string' || typeof item.stale !== 'boolean' || records.has(`work:${item.id}`))
      throw new Error('Invalid work freshness metadata');
    records.set(`work:${item.id}`, item.stale);
  }
  // Losing a configured source is a degradation even if the remaining sources
  // make the API report complete. Missing work never proves stale work recovered.
  for (const id of health.expectedSourceIds) if (!sourceIds.has(id)) records.set(`source:${id}`, true);
  const stale = [...records.entries()].filter(([, isStale]) => isStale).map(([id]) => id);
  if (inventory.coverage === 'partial' && !stale.length)
    throw new Error('Stale notice has no attributable source or work record');
  for (const id of stale) if (!Object.hasOwn(health.openEpisodes, id)) health.openEpisodes[id] = {since:elapsed};
  for (const [id, episode] of Object.entries(health.openEpisodes)) {
    const duration = elapsed - episode.since;
    health.maxEpisodeSeconds = Math.max(health.maxEpisodeSeconds, duration);
    if (duration > maximumPartialSeconds) throw new Error('Source or work degradation exceeded allowance');
    // Explicitly fresh same-ID evidence is the only recovery signal. A stale
    // timestamp update or disappearing row cannot reset this episode's clock.
    if (records.get(id) === false) {
      delete health.openEpisodes[id];
      health.recoveredEpisodes++;
    }
  }
  const partial = inventory.coverage === 'partial' || stale.length > 0 || Object.keys(health.openEpisodes).length > 0;
  health.lastCoverage = partial ? 'partial' : 'complete';
  if (partial) {
    health.partialSamples++;
    if (health.partialSince === null) health.partialSince = elapsed;
  } else {
    if (health.partialSince !== null)
      health.maxUnionPartialSeconds = Math.max(health.maxUnionPartialSeconds, elapsed - health.partialSince);
    health.completeSamples++;
    health.partialSince = null;
  }
  // Consecutive fleet-wide partial coverage is diagnostic only: overlapping
  // independently recovering records can legitimately keep its union open.
  if (health.partialSince !== null)
    health.maxUnionPartialSeconds = Math.max(health.maxUnionPartialSeconds, elapsed - health.partialSince);
}
