interface Props {
  connected: boolean;
  lastUpdate: Date | null;
}

export function ConnectionStatus({ connected, lastUpdate }: Props) {
  return (
    <div className="flex items-center gap-2 text-sm">
      <span
        className={`inline-block h-2 w-2 rounded-full ${
          connected ? "bg-emerald-400" : "bg-red-400 animate-pulse"
        }`}
      />
      <span className={connected ? "text-zinc-400" : "text-red-400"}>
        {connected ? "Connected" : "Disconnected"}
      </span>
      {lastUpdate && (
        <span className="text-zinc-500">
          {lastUpdate.toLocaleTimeString()}
        </span>
      )}
    </div>
  );
}
