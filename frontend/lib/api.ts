/**
 * TradeFlow NG — API Client
 * All backend endpoint calls, typed.
 *
 * Backend route map (after prefixes):
 *   POST /auth/admin/login   ← admin JWT
 *   POST /auth/agent/login   ← agent JWT  (was broken: /auth/auth/agent/login)
 */

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// ── Generic fetcher ───────────────────────────────────────────
async function apiFetch<T>(
  path: string,
  options: RequestInit = {},
  token?: string
): Promise<{ data: T; error: string | null; status: string }> {
  const headers: Record<string, string> = {
    "Content-Type": "application/json",
    ...(options.headers as Record<string, string>),
  };
  if (token) headers["Authorization"] = `Bearer ${token}`;

  try {
    const res = await fetch(`${API_BASE}${path}`, { ...options, headers });
    if (!res.ok) {
      const text = await res.text();
      let detail = text;
      try { detail = JSON.parse(text).detail || text; } catch {}
      return { data: null as T, error: detail, status: "error" };
    }
    return res.json();
  } catch (err) {
    return {
      data: null as T,
      error: "Cannot reach the TradeFlow NG API. Check your connection.",
      status: "error",
    };
  }
}

// ── Auth ──────────────────────────────────────────────────────
export async function adminLogin(username: string, password: string) {
  return apiFetch<{ access_token: string }>(
    "/auth/admin/login",           // → /auth/admin/login ✓
    { method: "POST", body: JSON.stringify({ username, password }) }
  );
}

export async function agentLogin(agent_id: string, password: string) {
  return apiFetch<{ access_token: string; agent_data: Record<string, unknown> }>(
    "/auth/agent/login",           // → /auth/agent/login ✓ (was /auth/auth/agent/login ✗)
    { method: "POST", body: JSON.stringify({ agent_id, password }) }
  );
}

// ── Admin ─────────────────────────────────────────────────────
export async function getOverview(token: string) {
  return apiFetch<Record<string, unknown>>("/admin/overview", {}, token);
}

export async function getRecommendations(
  token: string,
  params?: { commodity?: string; risk_only?: boolean; backhaul_only?: boolean }
) {
  const qs = new URLSearchParams();
  if (params?.commodity)     qs.set("commodity",     params.commodity);
  if (params?.risk_only)     qs.set("risk_only",     "true");
  if (params?.backhaul_only) qs.set("backhaul_only", "true");
  return apiFetch<unknown[]>(`/admin/recommendations?${qs}`, {}, token);
}

export async function getForecasts(token: string, state?: string, commodity?: string) {
  const qs = new URLSearchParams();
  if (state)     qs.set("state",     state);
  if (commodity) qs.set("commodity", commodity);
  return apiFetch<Record<string, unknown>>(`/forecasts/?${qs}`, {}, token);
}

export async function getPriceTrend(token: string, commodity = "Yam", days = 56) {
  return apiFetch<unknown[]>(
    `/admin/prices/trend?commodity=${encodeURIComponent(commodity)}&days=${days}`,
    {}, token
  );
}

export async function getTableau(token: string, commodity = "Yam") {
  return apiFetch<unknown[]>(
    `/admin/tableau?commodity=${encodeURIComponent(commodity)}`,
    {}, token
  );
}

export async function getAgents(token: string) {
  return apiFetch<unknown[]>("/admin/agents", {}, token);
}

export async function createAgent(token: string, body: Record<string, unknown>) {
  return apiFetch<Record<string, unknown>>(
    "/admin/agents",
    { method: "POST", body: JSON.stringify(body) },
    token
  );
}

export async function submitFeedback(token: string, body: Record<string, unknown>) {
  return apiFetch<Record<string, unknown>>(
    "/admin/feedback",
    { method: "POST", body: JSON.stringify(body) },
    token
  );
}

export async function getDbStats(token: string) {
  return apiFetch<Record<string, number>>("/admin/db/stats", {}, token);
}

export async function getLookups(token: string) {
  return apiFetch<Record<string, unknown[]>>("/admin/lookups", {}, token);
}

export async function getPipelineLogs(token: string) {
  return apiFetch<unknown[]>("/pipeline/logs", {}, token);
}

export async function triggerPipeline(token: string) {
  return apiFetch<Record<string, unknown>>("/pipeline/run", { method: "POST" }, token);
}

export async function triggerCleaning(token: string) {
  return apiFetch<Record<string, unknown>>("/pipeline/run/cleaning", { method: "POST" }, token);
}

export async function triggerForecasting(token: string) {
  return apiFetch<Record<string, unknown>>("/pipeline/run/forecasting", { method: "POST" }, token);
}

export async function triggerOptimization(token: string) {
  return apiFetch<Record<string, unknown>>("/pipeline/run/optimization", { method: "POST" }, token);
}

// ── Agent ─────────────────────────────────────────────────────
export async function getAgentRecommendations(token: string) {
  return apiFetch<unknown[]>("/agent/recommendations", {}, token);
}

export async function getAgentLocalPrices(token: string) {
  return apiFetch<unknown[]>("/agent/prices/local", {}, token);
}

export async function submitAgentPrice(token: string, body: Record<string, unknown>) {
  return apiFetch<Record<string, unknown>>(
    "/agent/prices/submit",
    { method: "POST", body: JSON.stringify(body) },
    token
  );
}

export async function getAgentSubmissions(token: string) {
  return apiFetch<unknown[]>("/agent/submissions/recent", {}, token);
}

export async function submitAgentReport(token: string, body: Record<string, unknown>) {
  return apiFetch<Record<string, unknown>>(
    "/agent/report",
    { method: "POST", body: JSON.stringify(body) },
    token
  );
}

export async function getAgentLookups(token: string) {
  return apiFetch<Record<string, unknown[]>>("/agent/lookups", {}, token);
}

// ── Public ────────────────────────────────────────────────────
export async function getLatestPrices() {
  return apiFetch<unknown[]>("/prices/latest");
}

export async function getHealth() {
  return apiFetch<Record<string, unknown>>("/health");
}

// ── Formatters ────────────────────────────────────────────────
export function formatNaira(value: number): string {
  return `₦${Math.round(value).toLocaleString("en-NG")}`;
}

export function formatDate(dateStr: string): string {
  const d = new Date(dateStr);
  return d.toLocaleDateString("en-GB", {
    day: "2-digit", month: "short", year: "numeric",
  });
}

export function formatPercent(value: number): string {
  return `${value.toFixed(1)}%`;
}

export const COMMODITY_ICONS: Record<string, string> = {
  Yam:    "🍠",
  Maize:  "🌽",
  Rice:   "🌾",
  Tomato: "🍅",
};
