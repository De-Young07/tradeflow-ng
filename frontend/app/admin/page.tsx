"use client";

import { useEffect, useState, useCallback } from "react";
import {
  ComposedChart, Line, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid,
} from "recharts";
import { getOverview, getPriceTrend, formatNaira, formatDate } from "@/lib/api";
import { getAdminToken } from "@/lib/auth";

export default function OverviewPage() {
  const token = getAdminToken() ?? "";
  const [data, setData] = useState<any>(null);
  const [trend, setTrend] = useState<Array<{date: string; price: number}>>([]);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    const [ov, tr] = await Promise.all([
      getOverview(token),
      getPriceTrend(token, "Yam", 28),
    ]);
    if (ov.data) setData(ov.data);
    if (tr.data) setTrend(tr.data as any[]);
    setLoading(false);
  }, [token]);

  useEffect(() => { load(); }, [load]);

  if (loading) return <div className="p-6">Loading…</div>;
  if (!data) return <div className="p-6">No overview data available.</div>;

  return (
    <div className="p-6 space-y-5 page-enter">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-heading font-bold text-gray-900">Overview</h1>
          <p className="text-sm text-gray-500 mt-0.5">System metrics and recent pipeline runs</p>
        </div>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-4 gap-4">
        <MetricCard label="Total profit" value={formatNaira(data.total_profit ?? 0)} />
        <MetricCard label="Recommendations" value={String(data.n_recommendations ?? 0)} />
        <MetricCard label="Agents" value={String(data.n_agents ?? 0)} />
        <MetricCard label="Price records" value={String(data.n_price_records ?? 0)} />
      </div>

      {/* Secondary metrics + mini chart */}
      <div className="grid grid-cols-3 gap-4">
        <div className="col-span-2 bg-white rounded-xl border border-gray-100 shadow-card p-4">
          <h2 className="text-base font-heading font-semibold text-gray-800 mb-4">Price trend — Yam (last 28 days)</h2>
          {trend.length === 0 ? (
            <div className="h-44 flex items-center justify-center text-gray-400">No trend data</div>
          ) : (
            <ResponsiveContainer width="100%" height={160}>
              <ComposedChart data={trend} margin={{ top: 5, right: 10, left: 0, bottom: 5 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
                <XAxis dataKey="date" tick={{ fontSize: 11, fill: "#9ca3af" }} tickFormatter={d => d.slice(5)} />
                <YAxis tick={{ fontSize: 11, fill: "#9ca3af" }} width={60} tickFormatter={v => `₦${(v/1000).toFixed(0)}k`} />
                <Tooltip formatter={(v: number) => formatNaira(v)} />
                <Line type="monotone" dataKey="price" stroke="#1A6B3C" strokeWidth={2} dot={false} />
              </ComposedChart>
            </ResponsiveContainer>
          )}
        </div>

        <div className="bg-white rounded-xl border border-gray-100 shadow-card p-4">
          <h3 className="text-sm font-medium text-gray-700 mb-2">System status</h3>
          <p className="text-sm text-gray-500">Last forecast: {data.last_forecast_date || "Never"}</p>
          <p className="text-sm text-gray-500">Last optimizer run: {data.last_optimization_date || "Never"}</p>
          <div className="mt-3">
            <h4 className="text-xs text-gray-500 uppercase tracking-wide">Recent pipeline runs</h4>
            <ul className="mt-2 text-sm">
              {(data.pipeline_logs || []).slice(0,5).map((l: any) => (
                <li key={l.id} className="py-1">
                  <div className="flex items-center justify-between">
                    <span className="text-gray-700">{l.run_type}</span>
                    <span className="text-xs text-gray-500">{l.status}{l.run_at ? ` • ${formatDate(l.run_at)}` : ""}</span>
                  </div>
                </li>
              ))}
              {(!data.pipeline_logs || data.pipeline_logs.length === 0) && (
                <li className="text-gray-400">No pipeline logs</li>
              )}
            </ul>
          </div>
        </div>
      </div>
    </div>
  );
}

function MetricCard({ label, value }: { label: string; value: string }) {
  return (
    <div className="bg-white rounded-xl border border-gray-100 shadow-card p-4">
      <p className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">{label}</p>
      <p className="text-xl font-heading font-bold text-gray-900 leading-snug">{value}</p>
    </div>
  );
}
