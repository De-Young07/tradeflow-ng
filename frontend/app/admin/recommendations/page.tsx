"use client";

import { useEffect, useState, useCallback } from "react";
import { AlertTriangle, ArrowRight, Download, RotateCcw } from "lucide-react";
import { getRecommendations, formatNaira, COMMODITY_ICONS } from "@/lib/api";
import { getAdminToken } from "@/lib/auth";
import clsx from "clsx";

const COMMODITIES = ["All", "Yam", "Maize", "Rice", "Tomato"];

type Rec = {
  id: number; commodity_name: string; origin: string; destination: string;
  recommended_quantity: number; buy_price: number; sell_price: number;
  transport_cost: number; profit_per_unit: number; expected_profit_ngn: number;
  profit_margin_pct: number; is_shock_flagged: boolean; is_backhaul: boolean;
  missing_cost_flag: boolean; shock_reason: string; backhaul_note: string; status: string;
};

export default function RecommendationsPage() {
  const token                   = getAdminToken() ?? "";
  const [recs, setRecs]         = useState<Rec[]>([]);
  const [loading, setLoading]   = useState(true);
  const [commodity, setCommodity] = useState("All");
  const [riskOnly, setRiskOnly] = useState(false);
  const [backhaulOnly, setBackhaulOnly] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    const res = await getRecommendations(token, {
      commodity:     commodity !== "All" ? commodity : undefined,
      risk_only:     riskOnly,
      backhaul_only: backhaulOnly,
    });
    if (res.data) setRecs(res.data as Rec[]);
    setLoading(false);
  }, [token, commodity, riskOnly, backhaulOnly]);

  useEffect(() => { load(); }, [load]);

  const totalProfit = recs.reduce((s, r) => s + r.expected_profit_ngn, 0);

  function exportCSV() {
    const headers = ["Commodity","Origin","Destination","Quantity","Buy","Sell","Transport","Profit/Unit","Total Profit","Margin%","Status"];
    const rows = recs.map(r => [
      r.commodity_name, r.origin, r.destination,
      r.recommended_quantity, r.buy_price, r.sell_price,
      r.transport_cost, r.profit_per_unit, r.expected_profit_ngn,
      r.profit_margin_pct, r.status
    ]);
    const csv = [headers, ...rows].map(r => r.join(",")).join("\n");
    const blob = new Blob([csv], { type: "text/csv" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `tradeflow-recommendations-${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
  }

  return (
    <div className="p-6 space-y-5 page-enter">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-heading font-bold text-gray-900">Recommendations</h1>
          <p className="text-sm text-gray-500 mt-0.5">Weekly optimized trade routes</p>
        </div>
        <button onClick={exportCSV} className="btn-secondary">
          <Download size={15} /> Export CSV
        </button>
      </div>

      {/* Filters */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-card p-4 flex flex-wrap gap-4 items-center">
        <div className="flex gap-1.5">
          {COMMODITIES.map(c => (
            <button
              key={c}
              onClick={() => setCommodity(c)}
              className={clsx(
                "px-3 py-1.5 rounded-lg text-sm font-medium transition-colors",
                commodity === c
                  ? "bg-green text-white"
                  : "bg-gray-100 text-gray-600 hover:bg-gray-200"
              )}
            >
              {c !== "All" && COMMODITY_ICONS[c]} {c}
            </button>
          ))}
        </div>
        <div className="flex gap-3 ml-auto">
          <Toggle label="⚠ High-Risk Only" active={riskOnly} onToggle={() => setRiskOnly(!riskOnly)} />
          <Toggle label="↩ Backhaul Only"   active={backhaulOnly} onToggle={() => setBackhaulOnly(!backhaulOnly)} />
        </div>
        <button onClick={load} className="btn-ghost p-2">
          <RotateCcw size={15} />
        </button>
      </div>

      {/* Cards */}
      {loading ? (
        <div className="space-y-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="rec-card h-48 shimmer" />
          ))}
        </div>
      ) : recs.length === 0 ? (
        <div className="bg-white rounded-xl border border-gray-100 shadow-card p-16 text-center">
          <p className="text-4xl mb-3">📊</p>
          <p className="text-gray-500 text-sm">No recommendations match the current filters.</p>
        </div>
      ) : (
        <div className="space-y-4">
          {recs.map(r => <RecCard key={r.id} rec={r} />)}
        </div>
      )}

      {/* Sticky total */}
      {!loading && recs.length > 0 && (
        <div className="sticky bottom-4 left-0 right-0 mx-auto max-w-2xl">
          <div className="bg-green text-white rounded-xl px-6 py-3 flex items-center justify-between shadow-lg">
            <span className="text-sm font-medium opacity-80">
              {recs.length} routes · {recs.filter(r => r.is_backhaul).length} backhauls
            </span>
            <span className="text-lg font-heading font-bold">
              {formatNaira(totalProfit)} expected
            </span>
          </div>
        </div>
      )}
    </div>
  );
}

function RecCard({ rec }: { rec: Rec }) {
  const icon = COMMODITY_ICONS[rec.commodity_name] ?? "📦";

  return (
    <div className="rec-card">
      {/* Heading */}
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-2">
          <span className="text-2xl">{icon}</span>
          <div>
            <h3 className="font-heading font-semibold text-gray-900">{rec.commodity_name}</h3>
            <div className="flex items-center gap-1.5 text-sm text-gray-500 mt-0.5">
              <span>{rec.origin}</span>
              <ArrowRight size={13} className="text-gray-400" />
              <span>{rec.destination}</span>
            </div>
          </div>
        </div>
        <div className="text-right">
          <p className="text-lg font-heading font-bold text-green">
            {formatNaira(rec.expected_profit_ngn)}
          </p>
          <p className="text-xs text-gray-400">{rec.profit_margin_pct.toFixed(1)}% margin</p>
        </div>
      </div>

      {/* Metrics grid */}
      <div className="grid grid-cols-3 md:grid-cols-6 gap-3 mb-4">
        {[
          { label: "Quantity",   value: `${rec.recommended_quantity.toFixed(0)} units` },
          { label: "Buy Price",  value: formatNaira(rec.buy_price) },
          { label: "Sell Price", value: formatNaira(rec.sell_price) },
          { label: "Transport",  value: formatNaira(rec.transport_cost) },
          { label: "Profit/Unit",value: formatNaira(rec.profit_per_unit) },
          { label: "Total",      value: formatNaira(rec.expected_profit_ngn), bold: true },
        ].map(m => (
          <div key={m.label} className="bg-gray-50 rounded-lg px-3 py-2.5">
            <p className="text-xs text-gray-400 mb-0.5">{m.label}</p>
            <p className={clsx("text-sm font-medium", m.bold ? "text-green font-semibold" : "text-gray-800")}>
              {m.value}
            </p>
          </div>
        ))}
      </div>

      {/* Flags */}
      <div className="space-y-2">
        {rec.is_shock_flagged && (
          <div className="strip-risk">
            <AlertTriangle size={13} />
            High-risk forecast — {rec.shock_reason || "price uncertainty detected"}
          </div>
        )}
        {rec.missing_cost_flag && (
          <div className="strip-risk">
            <AlertTriangle size={13} />
            No transport cost data for this corridor — profit estimate may be overstated
          </div>
        )}
        {rec.is_backhaul && rec.backhaul_note && (
          <div className="strip-backhaul">
            ↩ {rec.backhaul_note}
          </div>
        )}
      </div>
    </div>
  );
}

function Toggle({ label, active, onToggle }: { label: string; active: boolean; onToggle: () => void }) {
  return (
    <button
      onClick={onToggle}
      className={clsx(
        "px-3 py-1.5 rounded-lg text-xs font-medium border transition-colors",
        active
          ? "bg-gold-faint border-gold/40 text-gold"
          : "bg-white border-gray-200 text-gray-500 hover:border-gray-300"
      )}
    >
      {label}
    </button>
  );
}
