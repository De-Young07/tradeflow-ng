"use client";

import { useEffect, useState, useCallback } from "react";
import {
  ComposedChart, Line, Area, XAxis, YAxis, Tooltip,
  ResponsiveContainer, CartesianGrid, ReferenceLine, Legend,
} from "recharts";
import { getForecasts, formatNaira } from "@/lib/api";
import { getAdminToken } from "@/lib/auth";

const STATES      = ["Kwara","Lagos","Oyo","Ogun","Nasarawa","Niger","Abuja","Kogi"];
const COMMODITIES = ["Yam","Maize","Rice","Tomato"];

type ChartPoint = {
  date: string; price?: number;
  predicted?: number; lower?: number; upper?: number;
  isToday?: boolean;
};

export default function ForecastsPage() {
  const token = getAdminToken() ?? "";
  const [state, setState]         = useState("Kwara");
  const [commodity, setCommodity] = useState("Yam");
  const [chartData, setChartData] = useState<ChartPoint[]>([]);
  const [summary, setSummary]     = useState<Record<string, number>>({});
  const [loading, setLoading]     = useState(true);
  const [expanded, setExpanded]   = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    const res = await getForecasts(token, state, commodity);
    if (res.data) {
      const d = res.data as {
        historical: Array<{date: string; price: number}>;
        forecast:   Array<{date: string; predicted_price: number; lower_bound: number; upper_bound: number; is_shock_flagged: boolean}>;
        summary:    Record<string, number>;
      };
      const today = new Date().toISOString().slice(0, 10);

      const hist: ChartPoint[] = (d.historical || []).map(h => ({
        date: h.date, price: h.price,
      }));
      const fcast: ChartPoint[] = (d.forecast || []).map(f => ({
        date:      f.date,
        predicted: f.predicted_price,
        lower:     f.lower_bound,
        upper:     f.upper_bound,
      }));

      // Merge & mark today
      const merged = [...hist, ...fcast].map(p => ({
        ...p,
        isToday: p.date === today,
      }));

      setChartData(merged);
      setSummary(d.summary || {});
    }
    setLoading(false);
  }, [token, state, commodity]);

  useEffect(() => { load(); }, [load]);

  const forecastRows = chartData.filter(p => p.predicted !== undefined);

  return (
    <div className="p-6 space-y-5 page-enter">
      <div>
        <h1 className="text-2xl font-heading font-bold text-gray-900">Forecasts</h1>
        <p className="text-sm text-gray-500 mt-0.5">7-day Prophet price predictions with confidence intervals</p>
      </div>

      {/* Selectors */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-card p-4 flex flex-wrap gap-4">
        <div>
          <label className="form-label text-xs">State</label>
          <select value={state} onChange={e => setState(e.target.value)} className="form-select w-40 text-sm">
            {STATES.map(s => <option key={s}>{s}</option>)}
          </select>
        </div>
        <div>
          <label className="form-label text-xs">Commodity</label>
          <select value={commodity} onChange={e => setCommodity(e.target.value)} className="form-select w-36 text-sm">
            {COMMODITIES.map(c => <option key={c}>{c}</option>)}
          </select>
        </div>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-4">
        <SummaryCard
          label="Next Week Avg"
          value={formatNaira(summary.next_week_avg ?? 0)}
          sub="Predicted mean"
        />
        <SummaryCard
          label="Price Range"
          value={`${formatNaira(summary.price_range_low ?? 0)} – ${formatNaira(summary.price_range_high ?? 0)}`}
          sub="80% confidence band"
        />
        <SummaryCard
          label="High-Risk Days"
          value={String(summary.high_risk_days ?? 0)}
          sub="Shock-flagged in forecast"
          warn={(summary.high_risk_days ?? 0) > 0}
        />
      </div>

      {/* Chart */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-card p-6">
        <h2 className="text-base font-heading font-semibold text-gray-800 mb-5">
          {commodity} — {state} · Historical + 7-Day Forecast
        </h2>

        {loading ? (
          <div className="h-72 shimmer rounded-lg" />
        ) : chartData.length === 0 ? (
          <div className="h-72 flex items-center justify-center text-gray-400 text-sm">
            No forecast data available. Run the forecasting pipeline first.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height={300}>
            <ComposedChart data={chartData} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
              <XAxis
                dataKey="date"
                tick={{ fontSize: 11, fill: "#9ca3af" }}
                tickFormatter={d => d.slice(5)}
              />
              <YAxis
                tick={{ fontSize: 11, fill: "#9ca3af" }}
                tickFormatter={v => `₦${(v/1000).toFixed(0)}k`}
                width={55}
              />
              <Tooltip
                formatter={(v: number, name: string) => [
                  formatNaira(v),
                  name === "price" ? "Historical" :
                  name === "predicted" ? "Forecast" :
                  name === "upper" ? "Upper bound" : "Lower bound",
                ]}
                contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #e5e7eb" }}
              />
              <Legend
                formatter={name =>
                  name === "price" ? "Historical" :
                  name === "predicted" ? "Forecast" : null
                }
              />
              {/* Confidence band */}
              <Area
                type="monotone" dataKey="upper"
                stroke="transparent" fill="#1A6B3C" fillOpacity={0.1}
                name="upper"
              />
              <Area
                type="monotone" dataKey="lower"
                stroke="transparent" fill="#ffffff" fillOpacity={1}
                name="lower"
              />
              {/* Historical line */}
              <Line
                type="monotone" dataKey="price"
                stroke="#1A6B3C" strokeWidth={2.5}
                dot={false} name="price"
              />
              {/* Forecast line */}
              <Line
                type="monotone" dataKey="predicted"
                stroke="#C8860A" strokeWidth={2} strokeDasharray="6 3"
                dot={false} name="predicted"
              />
              {/* Today marker */}
              <ReferenceLine
                x={new Date().toISOString().slice(0, 10)}
                stroke="#9ca3af" strokeDasharray="4 3"
                label={{ value: "Today", fontSize: 10, fill: "#9ca3af", position: "top" }}
              />
            </ComposedChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Expandable table */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-card overflow-hidden">
        <button
          onClick={() => setExpanded(!expanded)}
          className="w-full px-6 py-4 flex items-center justify-between text-left border-b border-gray-100
                     hover:bg-gray-50/50 transition-colors"
        >
          <span className="text-sm font-heading font-semibold text-gray-700">
            7-Day Forecast Detail
          </span>
          <span className="text-xs text-gray-400">{expanded ? "▲ Collapse" : "▼ Expand"}</span>
        </button>
        {expanded && (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100">
                  {["Date","Predicted","Lower Bound","Upper Bound","Risk"].map(h => (
                    <th key={h} className="px-5 py-3 text-left text-xs font-medium text-gray-500 uppercase">
                      {h}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {forecastRows.map((row, i) => (
                  <tr key={i} className="border-b border-gray-50 hover:bg-gray-50/50">
                    <td className="px-5 py-3 font-medium text-gray-700">{row.date}</td>
                    <td className="px-5 py-3 text-green font-semibold">{formatNaira(row.predicted ?? 0)}</td>
                    <td className="px-5 py-3 text-gray-500">{formatNaira(row.lower ?? 0)}</td>
                    <td className="px-5 py-3 text-gray-500">{formatNaira(row.upper ?? 0)}</td>
                    <td className="px-5 py-3">
                      {row.upper && row.lower && (row.upper - row.lower) / (row.predicted ?? 1) > 0.3
                        ? <span className="badge-warning">⚠ High</span>
                        : <span className="badge-success">✓ Normal</span>
                      }
                    </td>
                  </tr>
                ))}
                {forecastRows.length === 0 && (
                  <tr>
                    <td colSpan={5} className="px-5 py-8 text-center text-gray-400 text-sm">
                      No forecast data available.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

function SummaryCard({ label, value, sub, warn = false }: {
  label: string; value: string; sub: string; warn?: boolean;
}) {
  return (
    <div className={warn ? "metric-card-gold" : "metric-card"}>
      <p className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">{label}</p>
      <p className="text-xl font-heading font-bold text-gray-900 leading-snug mb-0.5">{value}</p>
      <p className="text-xs text-gray-400">{sub}</p>
    </div>
  );
}
