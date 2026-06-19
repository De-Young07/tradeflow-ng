"use client";

import { useEffect, useState } from "react";
import { Loader2, CheckCircle } from "lucide-react";
import { getRecommendations, submitFeedback, formatNaira } from "@/lib/api";
import { getAdminToken } from "@/lib/auth";

type Rec = {
  id: number; commodity_name: string; origin: string;
  destination: string; buy_price: number; sell_price: number;
  expected_profit_ngn: number;
};

export default function FeedbackPage() {
  const token = getAdminToken() ?? "";
  const [recs, setRecs]       = useState<Rec[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [loading, setLoading] = useState(false);
  const [success, setSuccess] = useState(false);
  const [form, setForm] = useState({
    actual_buy_price: "", actual_sell_price: "", actual_transport_cost: "",
    actual_quantity:  "", trade_date: new Date().toISOString().slice(0, 10), notes: "",
  });

  useEffect(() => {
    getRecommendations(token).then(res => {
      if (res.data) setRecs(res.data as Rec[]);
    });
  }, [token]);

  const selected = recs.find(r => r.id === parseInt(selectedId));
  const buy    = parseFloat(form.actual_buy_price)       || 0;
  const sell   = parseFloat(form.actual_sell_price)      || 0;
  const trans  = parseFloat(form.actual_transport_cost)  || 0;
  const qty    = parseFloat(form.actual_quantity)        || 1;
  const actualProfit = (sell - buy - trans) * qty;

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!selectedId || !form.actual_buy_price || !form.actual_sell_price) {
      alert("Please select a route and fill buy/sell prices.");
      return;
    }
    setLoading(true);
    const res = await submitFeedback(token, {
      recommendation_id:     parseInt(selectedId),
      actual_buy_price:      parseFloat(form.actual_buy_price),
      actual_sell_price:     parseFloat(form.actual_sell_price),
      actual_transport_cost: form.actual_transport_cost ? parseFloat(form.actual_transport_cost) : null,
      actual_quantity:       form.actual_quantity ? parseFloat(form.actual_quantity) : null,
      trade_date:            form.trade_date,
      notes:                 form.notes || null,
    });
    if (res.error) { alert(res.error); setLoading(false); return; }
    setSuccess(true);
    setTimeout(() => setSuccess(false), 4000);
    setForm({ actual_buy_price: "", actual_sell_price: "", actual_transport_cost: "",
              actual_quantity: "", trade_date: new Date().toISOString().slice(0,10), notes: "" });
    setSelectedId("");
    setLoading(false);
  }

  return (
    <div className="p-6 max-w-2xl space-y-5 page-enter">
      <div>
        <h1 className="text-2xl font-heading font-bold text-gray-900">Trade Feedback</h1>
        <p className="text-sm text-gray-500 mt-0.5">
          Log actual trade outcomes to improve Prophet forecast accuracy
        </p>
      </div>

      {success && (
        <div className="flex items-center gap-2 px-4 py-3 rounded-xl text-sm font-medium text-green
                        bg-green-faint border border-green/20">
          <CheckCircle size={16} /> Outcome logged. Forecast model will retrain on next pipeline run.
        </div>
      )}

      <form onSubmit={handleSubmit} className="bg-white rounded-xl border border-gray-100 shadow-card p-6 space-y-5">
        <div>
          <label className="form-label">Select Recommendation</label>
          <select className="form-select" value={selectedId}
                  onChange={e => setSelectedId(e.target.value)}>
            <option value="">Choose a route…</option>
            {recs.map(r => (
              <option key={r.id} value={r.id}>
                {r.commodity_name} — {r.origin} → {r.destination}
              </option>
            ))}
          </select>
        </div>

        {selected && (
          <div className="p-4 rounded-xl bg-green-faint border border-green/15">
            <p className="text-xs text-gray-500 font-medium mb-2">System Prediction</p>
            <div className="grid grid-cols-3 gap-3 text-sm">
              <div><p className="text-xs text-gray-400">Buy</p><p className="font-medium">{formatNaira(selected.buy_price)}</p></div>
              <div><p className="text-xs text-gray-400">Sell</p><p className="font-medium">{formatNaira(selected.sell_price)}</p></div>
              <div><p className="text-xs text-gray-400">Expected Profit</p><p className="font-semibold text-green">{formatNaira(selected.expected_profit_ngn)}</p></div>
            </div>
          </div>
        )}

        <div className="grid grid-cols-2 gap-4">
          {[
            { key: "actual_buy_price",      label: "Actual Buy Price (₦) *",   ph: "22000" },
            { key: "actual_sell_price",     label: "Actual Sell Price (₦) *",  ph: "30000" },
            { key: "actual_transport_cost", label: "Actual Transport Cost (₦)", ph: "2500" },
            { key: "actual_quantity",       label: "Quantity Traded",           ph: "30" },
          ].map(f => (
            <div key={f.key}>
              <label className="form-label">{f.label}</label>
              <input type="number" className="form-input" placeholder={f.ph}
                     value={(form as Record<string, string>)[f.key]}
                     onChange={e => setForm(p => ({ ...p, [f.key]: e.target.value }))}
                     min="0" step="100" />
            </div>
          ))}
        </div>

        {(buy > 0 || sell > 0) && (
          <div className="p-4 rounded-xl bg-gold-faint border border-gold/20">
            <p className="text-xs font-medium text-gray-600 mb-2">Live Calculation</p>
            <div className="flex items-center gap-6">
              <div>
                <p className="text-xs text-gray-400">Actual Profit</p>
                <p className={`text-lg font-heading font-bold ${actualProfit >= 0 ? "text-green" : "text-red-600"}`}>
                  {formatNaira(actualProfit)}
                </p>
              </div>
              {selected && (
                <div>
                  <p className="text-xs text-gray-400">vs Expected</p>
                  <p className={`text-sm font-medium ${actualProfit >= selected.expected_profit_ngn ? "text-green" : "text-red-500"}`}>
                    {actualProfit >= selected.expected_profit_ngn ? "+" : ""}
                    {formatNaira(actualProfit - selected.expected_profit_ngn)}
                  </p>
                </div>
              )}
            </div>
          </div>
        )}

        <div>
          <label className="form-label">Trade Date</label>
          <input type="date" className="form-input" value={form.trade_date}
                 onChange={e => setForm(p => ({ ...p, trade_date: e.target.value }))} />
        </div>

        <div>
          <label className="form-label">Notes</label>
          <textarea className="form-input resize-none" rows={3}
                    placeholder="Market conditions, delays, price deviations, anything notable…"
                    value={form.notes}
                    onChange={e => setForm(p => ({ ...p, notes: e.target.value }))} />
        </div>

        <button type="submit" disabled={loading} className="btn-primary w-full">
          {loading ? <><Loader2 size={15} className="animate-spin" /> Saving…</> : "Log Trade Outcome"}
        </button>
      </form>
    </div>
  );
}
