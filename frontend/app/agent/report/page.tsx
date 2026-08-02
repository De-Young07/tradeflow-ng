"use client";

import { useEffect, useState } from "react";
import { Loader2, CheckCircle } from "lucide-react";
import {
  getAgentRecommendations,
  submitAgentReport,
  formatNaira,
  COMMODITY_ICONS,
} from "@/lib/api";
import { getAgentToken } from "@/lib/auth";

type Rec = {
  id: number;
  commodity_name: string;
  origin: string;
  destination: string;
  buy_price: number;
  sell_price: number;
  transport_cost: number;
  recommended_quantity: number;
};

export default function ReportOutcomePage() {
  const token = getAgentToken() ?? "";

  const [recs, setRecs]       = useState<Rec[]>([]);
  const [loadingRecs, setLoadingRecs] = useState(true);
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [form, setForm] = useState({
    recommendation_id: "",
    actual_buy_price: "",
    actual_sell_price: "",
    actual_transport_cost: "",
    actual_quantity: "",
    trade_date: new Date().toISOString().slice(0, 10),
    notes: "",
  });

  useEffect(() => {
    async function load() {
      setLoadingRecs(true);
      const res = await getAgentRecommendations(token);
      if (res.data) setRecs(res.data as Rec[]);
      setLoadingRecs(false);
    }
    load();
  }, [token]);

  // When a recommendation is picked, pre-fill the expected prices as sensible
  // starting points the agent can adjust to what actually happened.
  function selectRec(id: string) {
    const rec = recs.find(r => String(r.id) === id);
    setForm(p => ({
      ...p,
      recommendation_id: id,
      actual_buy_price:      rec ? String(Math.round(rec.buy_price)) : p.actual_buy_price,
      actual_sell_price:     rec ? String(Math.round(rec.sell_price)) : p.actual_sell_price,
      actual_transport_cost: rec ? String(Math.round(rec.transport_cost)) : p.actual_transport_cost,
      actual_quantity:       rec ? String(Math.round(rec.recommended_quantity)) : p.actual_quantity,
    }));
  }

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!form.recommendation_id) {
      alert("Please select the route you are reporting on.");
      return;
    }
    if (!form.actual_buy_price || !form.actual_sell_price) {
      alert("Please enter the actual buy and sell prices.");
      return;
    }
    setLoading(true);
    const res = await submitAgentReport(token, {
      recommendation_id:     parseInt(form.recommendation_id),
      actual_buy_price:      parseFloat(form.actual_buy_price),
      actual_sell_price:     parseFloat(form.actual_sell_price),
      actual_transport_cost: form.actual_transport_cost ? parseFloat(form.actual_transport_cost) : null,
      actual_quantity:       form.actual_quantity ? parseFloat(form.actual_quantity) : null,
      trade_date:            form.trade_date,
      notes:                 form.notes || null,
    });

    if (res.error) { alert(res.error); setLoading(false); return; }
    setSuccess(true);
    setTimeout(() => setSuccess(false), 3000);
    setForm({
      recommendation_id: "", actual_buy_price: "", actual_sell_price: "",
      actual_transport_cost: "", actual_quantity: "",
      trade_date: new Date().toISOString().slice(0, 10), notes: "",
    });
    setLoading(false);
  }

  return (
    <div className="px-4 pb-4 space-y-4">
      <div className="pt-5">
        <h2 className="text-xl font-heading font-bold text-white">Report Outcome</h2>
        <p className="text-white/50 text-sm mt-0.5">
          Log what actually happened on a trade route
        </p>
      </div>

      {/* Info note */}
      <div className="px-3 py-2.5 rounded-xl text-xs text-gold/80 leading-relaxed"
           style={{ background: "rgba(200,134,10,0.1)", border: "1px solid rgba(200,134,10,0.25)" }}>
        ℹ Reporting real outcomes helps the model learn. Enter the prices and
        quantities <strong>as they actually turned out</strong>, even if they
        differed from the recommendation.
      </div>

      {success && (
        <div className="flex items-center gap-2 px-4 py-3 rounded-xl text-sm font-medium text-green-300"
             style={{ background: "rgba(26,107,60,0.3)", border: "1px solid rgba(26,107,60,0.4)" }}>
          <CheckCircle size={16} /> Outcome reported successfully!
        </div>
      )}

      {loadingRecs ? (
        <div className="space-y-3">
          {[...Array(2)].map((_, i) => (
            <div key={i} className="h-12 rounded-xl shimmer opacity-20" />
          ))}
        </div>
      ) : recs.length === 0 ? (
        <div className="rounded-xl p-8 text-center" style={{ background: "rgba(255,255,255,0.05)" }}>
          <p className="text-3xl mb-2">📋</p>
          <p className="text-white/50 text-sm">No routes to report on yet.</p>
          <p className="text-white/30 text-xs mt-1">
            Outcomes are logged against this week&apos;s recommendations.
          </p>
        </div>
      ) : (
        <form onSubmit={handleSubmit} className="space-y-4">
          {/* Route selector */}
          <div>
            <label className="form-label text-white/70">Route *</label>
            <select
              className="form-select"
              value={form.recommendation_id}
              onChange={e => selectRec(e.target.value)}
            >
              <option value="">Select the route you traded…</option>
              {recs.map(r => (
                <option key={r.id} value={r.id}>
                  {COMMODITY_ICONS[r.commodity_name] ?? "📦"} {r.commodity_name}: {r.origin} → {r.destination}
                </option>
              ))}
            </select>
          </div>

          {/* Actual buy + sell */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="form-label text-white/70">Actual Buy (₦) *</label>
              <input
                type="number"
                className="form-input"
                placeholder="e.g. 18000"
                value={form.actual_buy_price}
                onChange={e => setForm(p => ({ ...p, actual_buy_price: e.target.value }))}
                min="0" step="100"
              />
            </div>
            <div>
              <label className="form-label text-white/70">Actual Sell (₦) *</label>
              <input
                type="number"
                className="form-input"
                placeholder="e.g. 95000"
                value={form.actual_sell_price}
                onChange={e => setForm(p => ({ ...p, actual_sell_price: e.target.value }))}
                min="0" step="100"
              />
            </div>
          </div>

          {/* Transport + Quantity */}
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="form-label text-white/70">Transport Cost (₦)</label>
              <input
                type="number"
                className="form-input"
                placeholder="e.g. 12000"
                value={form.actual_transport_cost}
                onChange={e => setForm(p => ({ ...p, actual_transport_cost: e.target.value }))}
                min="0" step="100"
              />
            </div>
            <div>
              <label className="form-label text-white/70">Quantity (units)</label>
              <input
                type="number"
                className="form-input"
                placeholder="e.g. 30"
                value={form.actual_quantity}
                onChange={e => setForm(p => ({ ...p, actual_quantity: e.target.value }))}
                min="0"
              />
            </div>
          </div>

          {/* Trade date */}
          <div>
            <label className="form-label text-white/70">Trade Date</label>
            <input
              type="date"
              className="form-input"
              value={form.trade_date}
              onChange={e => setForm(p => ({ ...p, trade_date: e.target.value }))}
            />
          </div>

          {/* Notes */}
          <div>
            <label className="form-label text-white/70">Notes (optional)</label>
            <textarea
              className="form-input resize-none"
              rows={2}
              placeholder="Anything that affected this trade — delays, spoilage, price swings…"
              value={form.notes}
              onChange={e => setForm(p => ({ ...p, notes: e.target.value }))}
            />
          </div>

          {/* Live profit preview */}
          {form.actual_buy_price && form.actual_sell_price && (
            <div className="px-4 py-3 rounded-xl text-sm"
                 style={{ background: "rgba(255,255,255,0.05)" }}>
              <span className="text-white/50">Net per unit: </span>
              <span className="text-gold font-semibold">
                {formatNaira(
                  parseFloat(form.actual_sell_price || "0") -
                  parseFloat(form.actual_buy_price || "0") -
                  parseFloat(form.actual_transport_cost || "0")
                )}
              </span>
            </div>
          )}

          <button type="submit" disabled={loading} className="btn-primary w-full h-12 text-base">
            {loading ? <><Loader2 size={16} className="animate-spin" /> Submitting…</> : "Report Outcome"}
          </button>
        </form>
      )}
    </div>
  );
}
