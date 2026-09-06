import { randomBytes, createHash } from "node:crypto";
import type { IncomingMessage } from "node:http";
import express, { type Express } from "express";
import { tokenMatches } from "./token.js";
import { requestIsSecure } from "./transport.js";

const COOKIE = "orcest_session";
const sessions = new Map<string, { expires: number; generation: string }>();
const connections = new Map<string, Set<() => void>>();
const failures = new Map<string, { count: number; expires: number }>();
const generation = () =>
  createHash("sha256")
    .update(process.env.DASHBOARD_TOKEN || "")
    .digest("hex");
function cookies(req: IncomingMessage): Record<string, string> {
  return Object.fromEntries(
    (req.headers.cookie || "").split(";").map((c) => {
      const i = c.indexOf("=");
      return [c.slice(0, i).trim(), c.slice(i + 1).trim()];
    }),
  );
}
export function sessionAuthorized(req: IncomingMessage): boolean {
  const id = cookies(req)[COOKIE];
  const session = sessions.get(id);
  if (!process.env.DASHBOARD_TOKEN || !session) return false;
  if (session.expires <= Date.now() || session.generation !== generation()) {
    sessions.delete(id);
    return false;
  }
  return true;
}
// A browser session authenticates both HTTP and ongoing output streams.
export function bindSessionSocket(
  req: IncomingMessage,
  ws: {
    close(code: number, reason: string): void;
    once(event: "close", listener: () => void): unknown;
  },
): void {
  const id = cookies(req)[COOKIE];
  if (!id || !sessionAuthorized(req)) return;
  const close = () => ws.close(1008, "Session ended");
  const set = connections.get(id) || new Set<() => void>();
  set.add(close);
  connections.set(id, set);
  const timer = setInterval(() => {
    if (!sessionAuthorized(req)) close();
  }, 1000);
  timer.unref();
  ws.once("close", () => {
    clearInterval(timer);
    set.delete(close);
    if (!set.size) connections.delete(id);
  });
}
function cookie(req: IncomingMessage, value: string, maxAge: number): string {
  const secure = requestIsSecure(req);
  return `${COOKIE}=${value}; Path=/; HttpOnly; SameSite=Strict; Max-Age=${maxAge}${secure ? "; Secure" : ""}`;
}
function sameOrigin(req: IncomingMessage): boolean {
  if (req.headers["sec-fetch-site"] === "cross-site") return false;
  const origin = req.headers.origin;
  if (!origin) return true; // CLI clients; browser JSON POSTs carry Origin.
  try {
    return (
      new URL(origin).host === req.headers.host ||
      (process.env.DASHBOARD_ALLOWED_ORIGINS || "")
        .split(",")
        .map((s) => s.trim())
        .includes(origin)
    );
  } catch {
    return false;
  }
}
export function installSignIn(app: Express): void {
  // Express walks X-Forwarded-For from the socket toward the client and stops
  // at the first untrusted hop. Never trust an arbitrary client-supplied header.
  app.set(
    "trust proxy",
    (process.env.DASHBOARD_TRUSTED_PROXIES || "")
      .split(",")
      .map((address) => address.trim())
      .filter(Boolean),
  );
  app.get("/sign-in", (_req, res) =>
    res
      .type("html")
      .set("Cache-Control", "no-store")
      .set(
        "Content-Security-Policy",
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'",
      )
      .send(PAGE),
  );
  app.get("/sign-in.js", (_req, res) => res.type("js").send(SCRIPT));
  app.get("/sign-in.css", (_req, res) => res.type("css").send(STYLE));
  app.post("/api/auth/login", express.json({ limit: "4kb" }), (req, res) => {
    res.set("Cache-Control", "no-store");
    if (!sameOrigin(req)) {
      res
        .status(403)
        .json({ error: "Sign-in must originate from this dashboard." });
      return;
    }
    if (!process.env.DASHBOARD_TOKEN) {
      res.status(503).json({ error: "Dashboard sign-in is not configured." });
      return;
    }
    const now = Date.now();
    for (const [key, entry] of failures)
      if (entry.expires <= now) failures.delete(key);
    const ip = req.ip || req.socket.remoteAddress || "unknown";
    const entry = failures.get(ip) || { count: 0, expires: now + 60000 };
    if (entry.count >= 10 || (!failures.has(ip) && failures.size >= 1000)) {
      res
        .set("Retry-After", "60")
        .status(429)
        .json({ error: "Too many attempts. Try again in a minute." });
      return;
    }
    if (
      typeof req.body?.token !== "string" ||
      !tokenMatches(req.body.token.trim())
    ) {
      entry.count++;
      failures.set(ip, entry);
      res.status(401).json({ error: "That access token was not recognized." });
      return;
    }
    failures.delete(ip);
    for (const [key, session] of sessions)
      if (session.expires <= now || session.generation !== generation())
        sessions.delete(key);
    if (sessions.size >= 500) {
      res
        .status(503)
        .json({ error: "Session capacity reached. Try again later." });
      return;
    }
    const id = randomBytes(32).toString("base64url");
    sessions.set(id, { expires: now + 12 * 3600000, generation: generation() });
    res.setHeader("Set-Cookie", cookie(req, id, 12 * 3600));
    res.json({ ok: true });
  });
  app.post("/api/auth/logout", (req, res) => {
    if (!sameOrigin(req)) {
      res.status(403).json({ error: "Invalid origin" });
      return;
    }
    const id = cookies(req)[COOKIE];
    sessions.delete(id);
    for (const close of connections.get(id) || []) close();
    connections.delete(id);
    res.setHeader("Set-Cookie", [
      cookie(req, "", 0),
      "orcest_dashboard_token=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0",
    ]);
    res.set("Cache-Control", "no-store").json({ ok: true });
  });
}
const PAGE = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Sign in · Orcest</title><link rel="stylesheet" href="/sign-in.css"><script src="/sign-in.js" defer></script></head><body><main><div class="brand">▱ orcest</div><h1>Sign in to your fleet</h1><p>Use the dashboard access token configured by your administrator.</p><form><label for="token">Access token</label><input id="token" name="token" type="password" autocomplete="current-password" required autofocus><p id="error" role="alert"></p><button type="submit">Sign in</button></form><small>Your token stays out of URLs and browser storage.</small></main></body></html>`;
const SCRIPT = `document.querySelector('form').addEventListener('submit',async function(e){e.preventDefault();const button=this.querySelector('button'),input=this.querySelector('input'),error=document.querySelector('#error');button.disabled=true;error.textContent='';try{const response=await fetch('/api/auth/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:input.value})});const data=await response.json();if(!response.ok)throw new Error(data.error||'Unable to sign in.');input.value='';location.replace('/');}catch(e){error.textContent=e.message||'Unable to reach the dashboard.';}finally{button.disabled=false;}});`;
const STYLE = `*{box-sizing:border-box}body{margin:0;min-height:100dvh;display:grid;place-items:center;background:#101314;color:#e9edeb;font:16px/1.5 system-ui,sans-serif;padding:24px}main{width:100%;max-width:410px}.brand{color:#85d6b5;font-weight:650;font-size:24px;margin-bottom:42px}h1{font-size:26px;letter-spacing:-.6px}p,small{color:#9ca8ae}label{display:block;margin:30px 0 8px}input{width:100%;background:#1b2023;border:1px solid #3a464a;border-radius:7px;padding:12px;color:inherit;font:inherit}input:focus-visible,button:focus-visible{outline:2px solid #85d6b5;outline-offset:3px}button{width:100%;background:#85d6b5;color:#11231b;border:0;border-radius:7px;padding:12px;font:inherit;font-weight:600;cursor:pointer}button:disabled{opacity:.5}#error{color:#f3b98a;min-height:24px;font-size:14px}small{display:block;margin-top:22px;font-size:13px}`;
