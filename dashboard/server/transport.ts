import type { IncomingMessage } from "node:http";

// Express resolves `secure` using the application's explicit trust-proxy
// policy. Raw HTTP requests may only establish HTTPS through their TLS socket.
export function requestIsSecure(req: IncomingMessage): boolean {
  return (
    (req as IncomingMessage & { secure?: boolean }).secure === true ||
    Boolean((req.socket as { encrypted?: boolean } | undefined)?.encrypted)
  );
}
