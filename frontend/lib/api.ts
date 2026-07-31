/**
 * TradeFlow NG — API Client
 * Fixed: backend returns tokens directly (no {data} wrapper on auth endpoints).
 * apiFetch now returns the raw response — login pages read it directly.
 */

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

// ── Generic fetcher ───────────────────────────────────────────
// Returns the raw parsed JSON from the backend.
// Auth endpoints return: { access_token, token_type, agent_data? }
// Data endpoints return: { data, error, status }
export async function apiFetch<T>(
  path: string,
  options: RequestInit = {},
  token?: string
): Promise<T> {
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
      // Return error shape consistent with data endpoints
      return { data: null, error: detail, status: "error" } as T;
    }

    return res.json() as T;
  } catch (err) {
    return {
      data:   null,
      error:  "Cannot reach the TradeFlow NG API. Check your connection.",
      status: "error",
    } as T;
  }
}

// ── Auth ──────────────────────────────────────────────────────
// These return tokens DIRECTLY — no {data} wrapper
export async function adminLogin(username: string, password: string) {
  return apiFetch<{
    access_token?: string;
    token_type?:   string;
    error?:        string;
  }>("/auth/admin/login", {
    method: "POST",
    body:   JSON.stringify({ username, password }),
  });
}

export async function agentLogin(agent_id: string, password: string) {
  return apiFetch<{
    access_token?: string;
    token_type?:   string;
    agent_data?:   Record<string, unknown>;
    error?:        string;
  }>("/auth/agent/login", {
    method: "POST",
    body:   JSON.stringify({ agent_id, password }),
  });
}

// ── Admin endpoints — return { data, error, status } ─────────
export async function getOverview(token: string) {
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    "/admin/overview", {}, token
  );
}

export async function getRecommendations(
  token: string,
  params?: { commodity?: string; risk_only?: boolean; backhaul_only?: boolean }
) {
  const qs = new URLSearchParams();
  if (params?.commodity)     qs.set("commodity",     params.commodity);
  if (params?.risk_only)     qs.set("risk_only",     "true");
  if (params?.backhaul_only) qs.set("backhaul_only", "true");
  return apiFetch<{ data: unknown[]; error: string | null }>(
    `/admin/recommendations?${qs}`, {}, token
  );
}

export async function getForecasts(
  token: string, state?: string, commodity?: string
) {
  const qs = new URLSearchParams();
  if (state)     qs.set("state",     state);
  if (commodity) qs.set("commodity", commodity);
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    `/forecasts/?${qs}`, {}, token
  );
}

export async function getPriceTrend(token: string, commodity = "Yam", days = 56) {
  return apiFetch<{ data: unknown[]; error: string | null }>(
    `/admin/prices/trend?commodity=${encodeURIComponent(commodity)}&days=${days}`,
    {}, token
  );
}

export async function getTableau(token: string, commodity = "Yam") {
  return apiFetch<{ data: unknown[]; error: string | null }>(
    `/admin/tableau?commodity=${encodeURIComponent(commodity)}`, {}, token
  );
}

export async function getAgents(token: string) {
  return apiFetch<{ data: unknown[]; error: string | null }>(
    "/admin/agents", {}, token
  );
}

export async function createAgent(token: string, body: Record<string, unknown>) {
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    "/admin/agents", { method: "POST", body: JSON.stringify(body) }, token
  );
}

export async function submitFeedback(token: string, body: Record<string, unknown>) {
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    "/admin/feedback", { method: "POST", body: JSON.stringify(body) }, token
  );
}

export async function getDbStats(token: string) {
  return apiFetch<{ data: Record<string, number>; error: string | null }>(
    "/admin/db/stats", {}, token
  );
}

export async function getLookups(token: string) {
  return apiFetch<{ data: Record<string, unknown[]>; error: string | null }>(
    "/admin/lookups", {}, token
  );
}

export async function getPipelineLogs(token: string) {
  return apiFetch<{ data: unknown[]; error: string | null }>(
    "/pipeline/logs", {}, token
  );
}

export async function triggerPipeline(token: string) {
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    "/pipeline/run", { method: "POST" }, token
  );
}

export async function triggerCleaning(token: string) {
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    "/pipeline/run/cleaning", { method: "POST" }, token
  );
}

export async function triggerForecasting(token: string) {
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    "/pipeline/run/forecasting", { method: "POST" }, token
  );
}

export async function triggerOptimization(token: string) {
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    "/pipeline/run/optimization", { method: "POST" }, token
  );
}

// ── Agent endpoints ───────────────────────────────────────────
export async function getAgentRecommendations(token: string) {
  return apiFetch<{ data: unknown[]; error: string | null }>(
    "/agent/recommendations", {}, token
  );
}

export async function getAgentLocalPrices(token: string) {
  return apiFetch<{ data: unknown[]; error: string | null }>(
    "/agent/prices/local", {}, token
  );
}

export async function submitAgentPrice(token: string, body: Record<string, unknown>) {
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    "/agent/prices/submit", { method: "POST", body: JSON.stringify(body) }, token
  );
}

export async function getAgentSubmissions(token: string) {
  return apiFetch<{ data: unknown[]; error: string | null }>(
    "/agent/submissions/recent", {}, token
  );
}

export async function submitAgentReport(token: string, body: Record<string, unknown>) {
  return apiFetch<{ data: Record<string, unknown>; error: string | null }>(
    "/agent/report", { method: "POST", body: JSON.stringify(body) }, token
  );
}

export async function getAgentLookups(token: string) {
  return apiFetch<{ data: Record<string, unknown[]>; error: string | null }>(
    "/agent/lookups", {}, token
  );
}

// ── Public ────────────────────────────────────────────────────
export async function getLatestPrices() {
  return apiFetch<{ data: unknown[]; error: string | null }>("/prices/latest");
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
