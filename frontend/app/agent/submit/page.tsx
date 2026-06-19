"use client";

import { useEffect, useState } from "react";
import { Loader2, CheckCircle } from "lucide-react";
import { submitAgentPrice, getAgentLookups, getAgentSubmissions, formatNaira, COMMODITY_ICONS } from "@/lib/api";
import { getAgentToken, getAgentData } from "@/lib/auth";

export default function SubmitPricePage() {
  const token     = getAgentToken() ?? "";
  const agentData = getAgentData();

  const [lookups, setLookups]       = useState<Record<string, unknown[]>>({});
  const [submissions, setSubmissions] = useState<unknown[]>([]);
  const [loading, setLoading]       = useState(false);
  const [success, setSuccess]       = useState(false);
  const [form, setForm] = useState({
    commodity_id: "", market_id: String(agentData?.market_id ?? ""),
    reported_price: "", quantity_available: "",
    quality_grade: "A", availability: "Available",
    road_condition: "Good", obs_date: new Date().toISOString().slice(0, 10),
    notes: "",
  });

  useEffect(() => {
    async function load() {
      const [lk, subs] = await Promise.all([
        getAgentLookups(token), getAgentSubmissions(token)
      ]);
      if (lk.data)   setLookups(lk.data as Record<string, unknown[]>);
      if (subs.data) setSubmissions(subs.data as unknown[]);
    }
    load();
  }, [token]);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!form.commodity_id || !form.reported_price) {
      alert("Please select a commodity and enter a price.");
      return;
    }
    setLoading(true);
    const res = await submitAgentPrice(token, {
      commodity_id:      parseInt(form.commodity_id),
      market_id:         form.market_id ? parseInt(form.market_id) : null,
      reported_price:    parseFloat(form.reported_price),
      quantity_available: form.quantity_available ? parseFloat(form.quantity_available) : null,
      quality_grade:     form.quality_grade,
      road_condition:    form.road_condition,
      obs_date:          form.obs_date,
      notes:             form.notes || null,
    });

    if (res.error) { alert(res.error); setLoading(false); return; }
    setSuccess(true);
    setTimeout(() => setSuccess(false), 3000);
    setForm(prev => ({ ...prev, commodity_id: "", reported_price: "", quantity_available: "", notes: "" }));

    // Refresh submissions
    const subs = await getAgentSubmissions(token);
    if (subs.data) setSubmissions(subs.data as unknown[]);
    setLoading(false);
  }

  const commodities = (lookups.commodities ?? []) as Array<{id: number; name: string}>;
  const markets     = (lookups.markets     ?? []) as Array<{id: number; name: string}>;

  return (
    <div className="px-4 pb-4 space-y-4">
      <div className="pt-5">
        <h2 className="text-xl font-heading font-bold text-white">Submit Price</h2>
        <p className="text-white/50 text-sm mt-0.5">Report this week&apos;s commodity prices</p>
      </div>

      {/* Retail note */}
      <div className="px-3 py-2.5 rounded-xl text-xs text-gold/80 leading-relaxed"
           style={{ background: "rgba(200,134,10,0.1)", border: "1px solid rgba(200,134,10,0.25)" }}>
        ℹ Submit <strong>retail market prices</strong> as observed in your market.
        The system applies wholesale adjustments automatically.
      </div>

      {success && (
        <div className="flex items-center gap-2 px-4 py-3 rounded-xl text-sm font-medium text-green-300"
             style={{ background: "rgba(26,107,60,0.3)", border: "1px solid rgba(26,107,60,0.4)" }}>
          <CheckCircle size={16} /> Price submitted successfully!
        </div>
      )}

      <form onSubmit={handleSubmit} className="space-y-4">
        {/* Commodity */}
        <div>
          <label className="form-label text-white/70">Commodity *</label>
          <select
            className="form-select"
            value={form.commodity_id}
            onChange={e => setForm(p => ({ ...p, commodity_id: e.target.value }))}
          >
            <option value="">Select commodity…</option>
            {commodities.map(c => (
              <option key={c.id} value={c.id}>
                {COMMODITY_ICONS[c.name] ?? "📦"} {c.name}
              </option>
            ))}
          </select>
        </div>

        {/* Market */}
        <div>
          <label className="form-label text-white/70">Market</label>
          <select
            className="form-select"
            value={form.market_id}
            onChange={e => setForm(p => ({ ...p, market_id: e.target.value }))}
          >
            <option value="">Select market…</option>
            {markets.map(m => (
              <option key={m.id} value={m.id}>{m.name}</option>
            ))}
          </select>
        </div>

        {/* Price + Quantity */}
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="form-label text-white/70">Price (₦) *</label>
            <input
              type="number"
              className="form-input"
              placeholder="e.g. 25000"
              value={form.reported_price}
              onChange={e => setForm(p => ({ ...p, reported_price: e.target.value }))}
              min="0" step="100"
            />
          </div>
          <div>
            <label className="form-label text-white/70">Quantity (units)</label>
            <input
              type="number"
              className="form-input"
              placeholder="e.g. 50"
              value={form.quantity_available}
              onChange={e => setForm(p => ({ ...p, quantity_available: e.target.value }))}
              min="0"
            />
          </div>
        </div>

        {/* Quality + Availability */}
        <div className="grid grid-cols-2 gap-3">
          <div>
            <label className="form-label text-white/70">Quality Grade</label>
            <select className="form-select" value={form.quality_grade}
                    onChange={e => setForm(p => ({ ...p, quality_grade: e.target.value }))}>
              {["A", "B", "C"].map(g => <option key={g}>Grade {g}</option>)}
            </select>
          </div>
          <div>
            <label className="form-label text-white/70">Road Condition</label>
            <select className="form-select" value={form.road_condition}
                    onChange={e => setForm(p => ({ ...p, road_condition: e.target.value }))}>
              {["Good", "Fair", "Poor"].map(c => <option key={c}>{c}</option>)}
            </select>
          </div>
        </div>

        {/* Date */}
        <div>
          <label className="form-label text-white/70">Observation Date</label>
          <input type="date" className="form-input" value={form.obs_date}
                 onChange={e => setForm(p => ({ ...p, obs_date: e.target.value }))} />
        </div>

        {/* Notes */}
        <div>
          <label className="form-label text-white/70">Notes (optional)</label>
          <textarea
            className="form-input resize-none"
            rows={2}
            placeholder="Any unusual market activity this week…"
            value={form.notes}
            onChange={e => setForm(p => ({ ...p, notes: e.target.value }))}
          />
        </div>

        <button type="submit" disabled={loading} className="btn-primary w-full h-12 text-base">
          {loading ? <><Loader2 size={16} className="animate-spin" /> Submitting…</> : "Submit Price Report"}
        </button>
      </form>

      {/* Recent submissions */}
      {submissions.length > 0 && (
        <div>
          <p className="text-white/50 text-xs font-medium uppercase tracking-wide mb-2">Recent Submissions</p>
          <div className="space-y-2">
            {(submissions as Array<Record<string, unknown>>).map((s, i) => (
              <div key={i} className="flex items-center justify-between px-4 py-3 rounded-xl"
                   style={{ background: "rgba(255,255,255,0.05)" }}>
                <div>
                  <p className="text-white text-sm font-medium">
                    {COMMODITY_ICONS[String(s.commodity)] ?? "📦"} {String(s.commodity)}
                  </p>
                  <p className="text-white/40 text-xs">{String(s.submission_date)} · {String(s.market ?? "—")}</p>
                </div>
                <p className="text-gold font-semibold text-sm">{formatNaira(Number(s.reported_price))}</p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
