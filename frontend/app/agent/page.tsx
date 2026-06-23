"use client";

import { useEffect, useState } from "react";
import { ArrowRight, AlertTriangle } from "lucide-react";
import { getAgentRecommendations, formatNaira, COMMODITY_ICONS } from "@/lib/api";
import { getAgentToken, getAgentData } from "@/lib/auth";
import clsx from "clsx";

type Rec = {
  id: number; commodity_name: string; origin: string; destination: string;
  recommended_quantity: number; buy_price: number; sell_price: number;
  transport_cost: number; expected_profit_ngn: number; profit_margin_pct: number;
  is_shock_flagged: boolean; is_backhaul: boolean; backhaul_note: string; status: string;
};

export default function AgentPage() {
  const token      = getAgentToken() ?? "";
  const agentData  = getAgentData();
  const [recs, setRecs]       = useState<Rec[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      setLoading(true);
      const res = await getAgentRecommendations(token);
      if (res.data) setRecs(res.data as Rec[]);
      setLoading(false);
    }
    load();
  }, [token]);

  const firstName  = String(agentData?.name ?? "Agent").split(" ")[0];
  const weekNumber = getWeekNumber();

  return (
    <div className="px-4 space-y-4">
      {/* Hero banner */}
      <div className="pt-4 pb-5">
        <div
          className="rounded-2xl p-5"
          style={{ background: "rgba(255,255,255,0.06)", border: "1px solid rgba(255,255,255,0.1)" }}
        >
          <p className="text-white/60 text-sm mb-1">Week {weekNumber}</p>
          <h1 className="text-2xl font-heading font-bold text-white mb-1">
            Hello, {firstName}!
          </h1>
          <div className="flex flex-wrap items-center gap-2 mt-2">
            {!!agentData?.market && (
              <span className="text-xs px-2.5 py-1 rounded-full bg-white/10 text-white/70">
                📍 {String(agentData.market)}
              </span>
            )}
            {agentData?.agent_id ? (
              <span className="text-xs px-2.5 py-1 rounded-full font-mono text-gold"
                    style={{ background: "rgba(200,134,10,0.15)", border: "1px solid rgba(200,134,10,0.3)" }}>
                {String(agentData.agent_id)}
              </span>
            ) : null}
            

            {!!agentData?.agent_id && (
              <span className="text-xs px-2.5 py-1 rounded-full font-mono text-gold"
                    style={{ background: "rgba(200,134,10,0.15)", border: "1px solid rgba(200,134,10,0.3)" }}>
                {String(agentData.agent_id)}
              </span>
            )}
          </div>
        </div>
      </div>

      {/* Retail note */}
      <div className="strip-info mx-0">
        <span>ℹ</span>
        <span>
          These are wholesale-optimised routes. Retail prices in your market will differ.
          Prices shown reflect an 18% wholesale discount from retail.
        </span>
      </div>

      {/* Recommendations */}
      <div className="pb-4">
        <p className="text-white/60 text-xs font-medium uppercase tracking-wide mb-3">
          Routes for {agentData?.state ?? "Your State"} · This Week
        </p>

        {loading ? (
          <div className="space-y-3">
            {[...Array(3)].map((_, i) => (
              <div key={i} className="h-40 rounded-xl shimmer opacity-20" />
            ))}
          </div>
        ) : recs.length === 0 ? (
          <div className="rounded-xl p-10 text-center"
               style={{ background: "rgba(255,255,255,0.05)" }}>
            <p className="text-3xl mb-2">📊</p>
            <p className="text-white/50 text-sm">No active routes for your state this week.</p>
            <p className="text-white/30 text-xs mt-1">Check back after the next pipeline run.</p>
          </div>
        ) : (
          <div className="space-y-3">
            {recs.map(r => <AgentRecCard key={r.id} rec={r} />)}
          </div>
        )}
      </div>
    </div>
  );
}

function AgentRecCard({ rec }: { rec: Rec }) {
  const icon = COMMODITY_ICONS[rec.commodity_name] ?? "📦";

  return (
    <div
      className="rounded-xl p-4"
      style={{
        background: "rgba(255,255,255,0.07)",
        border: "1px solid rgba(255,255,255,0.1)",
        borderLeft: "4px solid #1A6B3C",
      }}
    >
      <div className="flex items-start justify-between mb-3">
        <div>
          <div className="flex items-center gap-2">
            <span className="text-xl">{icon}</span>
            <span className="text-white font-heading font-semibold">{rec.commodity_name}</span>
            {rec.is_shock_flagged && (
              <span className="text-xs px-1.5 py-0.5 rounded bg-red-500/20 text-red-300 font-medium">
                ⚠ Risk
              </span>
            )}
          </div>
          <div className="flex items-center gap-1.5 text-sm text-white/50 mt-0.5">
            <span>{rec.origin}</span>
            <ArrowRight size={12} />
            <span>{rec.destination}</span>
          </div>
        </div>
        <div className="text-right">
          <p className="text-lg font-heading font-bold text-gold">
            {formatNaira(rec.expected_profit_ngn)}
          </p>
          <p className="text-xs text-white/40">{rec.profit_margin_pct.toFixed(1)}% margin</p>
        </div>
      </div>

      <div className="grid grid-cols-3 gap-2">
        {[
          { label: "Buy",      value: formatNaira(rec.buy_price) },
          { label: "Sell",     value: formatNaira(rec.sell_price) },
          { label: "Qty",      value: `${rec.recommended_quantity.toFixed(0)} units` },
        ].map(m => (
          <div key={m.label} className="rounded-lg px-2.5 py-2"
               style={{ background: "rgba(255,255,255,0.05)" }}>
            <p className="text-xs text-white/40 mb-0.5">{m.label}</p>
            <p className="text-sm font-medium text-white">{m.value}</p>
          </div>
        ))}
      </div>

      {rec.is_backhaul && rec.backhaul_note && (
        <div className="mt-3 px-3 py-2 rounded-lg text-xs text-gold/80"
             style={{ background: "rgba(200,134,10,0.1)", border: "1px solid rgba(200,134,10,0.2)" }}>
          ↩ {rec.backhaul_note}
        </div>
      )}
    </div>
  );
}

function getWeekNumber(): number {
  const now  = new Date();
  const start = new Date(now.getFullYear(), 0, 1);
  const diff  = now.getTime() - start.getTime();
  return Math.ceil(diff / (7 * 24 * 60 * 60 * 1000));
}
