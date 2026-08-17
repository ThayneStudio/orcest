export function degradedCountLabel(count: number, degraded: boolean): string {
  if (!degraded) return String(count);
  return count > 0 ? `${count} loaded` : "?";
}

export function loadedCountLabel(count: number, _unknownWhenEmpty = false): string {
  return `${count} loaded`;
}
