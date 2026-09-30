// Pure qualification rules shared by the collector and its regression checks.
export function qualifies(summary) {
  return summary.status === 'finished' && !summary.synthetic && summary.failures.length === 0 &&
    summary.elapsedSeconds >= 86400 && Number.isFinite(summary.cadenceSeconds) &&
    summary.cadenceSeconds >= 5 && summary.cadenceSeconds <= 60 &&
    summary.samples >= Math.ceil(86400 / Math.max(summary.cadenceSeconds * 1.5, 30)) + 1 && summary.sessionExpiryObserved &&
    summary.logoutVerified && summary.coverageHealth?.completeSamples > 0 &&
    summary.coverageHealth.partialSince === null && summary.outputAttempts.length > 0 &&
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
    (!work.headSha || work.headSha === pr.headRefOid);
}

const STALE_NOTICE = 'Some source observations are stale. Last-known work is retained.';
export function observeCoverage(health, inventory, elapsed, maximumPartialSeconds) {
  if (inventory.coverage === 'unavailable') throw new Error('Fleet observations unavailable');
  if (inventory.coverage === 'complete') {
    if (health.partialSince !== null) {
      if (elapsed - health.partialSince > maximumPartialSeconds) throw new Error('Source degradation exceeded allowance');
      health.recoveredEpisodes++;
      health.maxPartialSeconds = Math.max(health.maxPartialSeconds, elapsed - health.partialSince);
      health.partialSince = null;
    }
    health.completeSamples++;
    return;
  }
  if (inventory.coverage !== 'partial' || !inventory.notices.length ||
    inventory.notices.some(notice => notice !== STALE_NOTICE)) throw new Error('Unexplained degraded coverage');
  health.partialSamples++;
  if (health.partialSince === null) health.partialSince = elapsed;
  const duration = elapsed - health.partialSince;
  health.maxPartialSeconds = Math.max(health.maxPartialSeconds, duration);
  if (duration > maximumPartialSeconds) throw new Error('Source degradation exceeded allowance');
}
