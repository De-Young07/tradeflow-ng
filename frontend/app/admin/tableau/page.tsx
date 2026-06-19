"use client";

import { useEffect, useState, useCallback } from "react";
import { getTableau, getRecommendations, formatNaira } from "@/lib/api";
import { getAdminToken } from "@/lib/auth";
import clsx from "clsx";

const STATES      = ["Nasarawa","Niger","Abuja","Kwara","Kogi","Lagos","Oyo","Ogun"];
const COMMODITIES = ["Yam","Maize","Rice","Tomato"];

type Cell = {
  origin: string; destination: string;
  profit_per_unit: number; margin_pct: number; is_profitable: boolean;
};

export default function TableauPage() {
  const token = getAdminToken() ?? "";
  const [commodity, setCommodity] = useState("Yam");
  const [matrix, setMatrix]       = useState<Cell[]>([]);
  const [topRoutes, setTopRoutes] = useState<unknown[]>([]);
  const [loading, setLoading]     = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    const [mat, recs] = await Promise.all([
      getTableau(token, commodity),
      getRecommendations(token, { commodity }),
    ]);
    if (mat.data)  setMatrix(mat.data as Cell[]);
    if (recs.data) setTopRoutes((recs.data as unknown[]).slice(0, 3));
    setLoading(false);
  }, [token, commodity]);

  useEffect(() => { load(); }, [load]);

  // Build lookup for fast cell access
  const cellMap: Record<string, Cell> = {};
  matrix.forEach(c => { cellMap[`${c.origin}|${c.destination}`] = c; });

  // Color interpolation: red → white → green
  function cellColor(profit: number | undefined): string {
    if (profit === undefined) return "#F9FAFB";
    if (profit <= 0) return `rgba(220,38,38,${Math.min(Math.abs(profit) / 5000, 0.8)})`;
    return `rgba(26,107,60,${Math.min(profit / 8000, 0.85)})`;
  }

  function cellText(profit: number | undefined): string {
    if (profit === undefined) return "—";
    return formatNaira(profit);
  }

  const origins = ["Nasarawa","Niger","Abuja","Kogi","Kwara"];
  const dests   = ["Lagos","Oyo","Ogun","Kwara","Abuja"];

  return (
    <div className="p-6 space-y-5 page-enter">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-heading font-bold text-gray-900">Profit Matrix</h1>
          <p className="text-sm text-gray-500 mt-0.5">Profit per unit across all trade corridors</p>
        </div>
        <select
          value={commodity}
          onChange={e => setCommodity(e.target.value)}
          className="form-select w-36"
        >
          {COMMODITIES.map(c => <option key={c}>{c}</option>)}
        </select>
      </div>

      {/* Heatmap */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-card p-6">
        <h2 className="text-sm font-heading font-semibold text-gray-700 mb-4">
          {commodity} — Profit per Unit (₦) by Corridor
        </h2>

        {loading ? (
          <div className="h-64 shimmer rounded-lg" />
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full border-collapse text-xs">
              <thead>
                <tr>
                  <th className="p-2 text-left text-gray-400 font-medium w-28">Origin → Dest</th>
                  {dests.map(d => (
                    <th key={d} className="p-2 text-center font-medium text-gray-600 min-w-[100px]">
                      {d}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {origins.map(origin => (
                  <tr key={origin}>
                    <td className="p-2 font-medium text-gray-700 text-xs">{origin}</td>
                    {dests.map(dest => {
                      if (origin === dest) {
                        return <td key={dest} className="p-1.5"><div className="h-12 rounded-lg bg-gray-50" /></td>;
                      }
                      const cell = cellMap[`${origin}|${dest}`];
                      const profit = cell?.profit_per_unit;
                      return (
                        <td key={dest} className="p-1.5">
                          <div
                            className="h-12 rounded-lg flex items-center justify-center font-semibold text-xs
                                       transition-transform hover:scale-105 cursor-default"
                            style={{
                              background: cellColor(profit),
                              color: profit !== undefined && Math.abs(profit) > 2000 ? "white" : "#374151",
                            }}
                            title={`${origin} → ${dest}: ${cellText(profit)}`}
                          >
                            {cellText(profit)}
                          </div>
                        </td>
                      );
                    })}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}

        {/* Legend */}
        <div className="flex items-center gap-4 mt-4">
          <div className="flex items-center gap-2 text-xs text-gray-500">
            <div className="w-4 h-4 rounded" style={{ background: "rgba(220,38,38,0.6)" }} />
            Unprofitable
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-500">
            <div className="w-4 h-4 rounded bg-gray-100" />
            No data
          </div>
          <div className="flex items-center gap-2 text-xs text-gray-500">
            <div className="w-4 h-4 rounded" style={{ background: "rgba(26,107,60,0.7)" }} />
            Profitable
          </div>
        </div>
      </div>

      {/* Top 3 routes */}
      {topRoutes.length > 0 && (
        <div className="bg-white rounded-xl border border-gray-100 shadow-card p-6">
          <h2 className="text-sm font-heading font-semibold text-gray-700 mb-4">
            Top {topRoutes.length} {commodity} Routes
          </h2>
          <div className="space-y-3">
            {(topRoutes as Array<Record<string, unknown>>).map((r, i) => (
              <div key={i} className="flex items-center justify-between p-4 rounded-xl bg-green-faint">
                <div className="flex items-center gap-3">
                  <span className="text-lg font-heading font-bold text-green opacity-40">
                    #{i + 1}
                  </span>
                  <div>
                    <p className="font-medium text-gray-800 text-sm">
                      {String(r.origin)} → {String(r.destination)}
                    </p>
                    <p className="text-xs text-gray-500">
                      {Number(r.recommended_quantity).toFixed(0)} units · {Number(r.profit_margin_pct).toFixed(1)}% margin
                    </p>
                  </div>
                </div>
                <p className="font-heading font-bold text-green">
                  {formatNaira(Number(r.expected_profit_ngn))}
                </p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
