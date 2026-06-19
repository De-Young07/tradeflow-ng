/**
 * TradeFlow NG — Auth Utilities
 * JWT storage and session helpers for Next.js.
 */

import { SignJWT, jwtVerify } from "jose";

const JWT_SECRET = new TextEncoder().encode(
  process.env.JWT_SECRET || "tradeflow-dev-secret-change-in-prod"
);

const ADMIN_TOKEN_KEY = "tf_admin_token";
const AGENT_TOKEN_KEY = "tf_agent_token";
const AGENT_DATA_KEY  = "tf_agent_data";

// ── Token storage (client-side) ───────────────────────────────
export function saveAdminToken(token: string) {
  if (typeof window !== "undefined") {
    sessionStorage.setItem(ADMIN_TOKEN_KEY, token);
  }
}

export function getAdminToken(): string | null {
  if (typeof window !== "undefined") {
    return sessionStorage.getItem(ADMIN_TOKEN_KEY);
  }
  return null;
}

export function saveAgentToken(token: string, agentData: Record<string, unknown>) {
  if (typeof window !== "undefined") {
    sessionStorage.setItem(AGENT_TOKEN_KEY, token);
    sessionStorage.setItem(AGENT_DATA_KEY, JSON.stringify(agentData));
  }
}

export function getAgentToken(): string | null {
  if (typeof window !== "undefined") {
    return sessionStorage.getItem(AGENT_TOKEN_KEY);
  }
  return null;
}

export function getAgentData(): Record<string, unknown> | null {
  if (typeof window !== "undefined") {
    const raw = sessionStorage.getItem(AGENT_DATA_KEY);
    if (raw) {
      try { return JSON.parse(raw); } catch {}
    }
  }
  return null;
}

export function clearAdminSession() {
  if (typeof window !== "undefined") {
    sessionStorage.removeItem(ADMIN_TOKEN_KEY);
  }
}

export function clearAgentSession() {
  if (typeof window !== "undefined") {
    sessionStorage.removeItem(AGENT_TOKEN_KEY);
    sessionStorage.removeItem(AGENT_DATA_KEY);
  }
}

export function isAdminAuthenticated(): boolean {
  return !!getAdminToken();
}

export function isAgentAuthenticated(): boolean {
  return !!getAgentToken();
}

// ── Server-side: verify JWT from cookie (middleware) ──────────
export async function verifyJWT(token: string): Promise<{
  role: string;
  sub: string;
  [key: string]: unknown;
} | null> {
  try {
    const { payload } = await jwtVerify(token, JWT_SECRET);
    return payload as { role: string; sub: string; [key: string]: unknown };
  } catch {
    return null;
  }
}

// ── Cookie helpers for middleware ─────────────────────────────
export const ADMIN_COOKIE = "tf_admin";
export const AGENT_COOKIE = "tf_agent";
