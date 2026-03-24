export function formatTtl(seconds: number): string {
  if (seconds === -1) return "no TTL";
  if (seconds < 0) return "expired";
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (minutes < 60) return `${minutes}m ${secs}s`;
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return `${hours}h ${mins}m`;
}

export function formatDuration(seconds: number): string {
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const secs = seconds % 60;
  if (minutes < 60) return `${minutes}m ${secs}s`;
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  return `${hours}h ${mins}m`;
}

export function statusColor(status: string): string {
  switch (status.toLowerCase()) {
    case "completed": return "bg-emerald-500/20 text-emerald-400";
    case "failed": return "bg-red-500/20 text-red-400";
    case "blocked": return "bg-yellow-500/20 text-yellow-400";
    case "usage_exhausted": return "bg-purple-500/20 text-purple-400";
    default: return "bg-zinc-500/20 text-zinc-400";
  }
}

export function timeAgo(iso: string | null): string {
  if (!iso) return "";
  const diff = Date.now() - new Date(iso).getTime();
  const mins = Math.floor(diff / 60000);
  if (mins < 1) return "just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.floor(hours / 24)}d ago`;
}
