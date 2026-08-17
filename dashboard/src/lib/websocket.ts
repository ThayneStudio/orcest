const ABNORMAL_CLOSE_CODE = 1006;

export function abnormalCloseMessage(
  code: number,
  consecutiveFailures: number,
  threshold: number,
  label: string,
): string | null {
  if (code !== ABNORMAL_CLOSE_CODE || consecutiveFailures < threshold) {
    return null;
  }

  return `${label} connection could not be opened. Refresh with a valid dashboard token or check connectivity.`;
}
