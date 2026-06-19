"use client";

import { useEffect, useState } from "react";
import { Loader2, CheckCircle, Play, Database, BarChart2, Zap } from "lucide-react";
import {
  triggerCleaning, triggerForecasting, triggerOptimization,
  triggerPipeline, getDbStats, getLookups, createAgent,
} from "@/lib/api";
import { getAdminToken } from "@/lib/auth";
import toast, { Toaster } from "react-hot-toast";

export default function DataPage() {
  const token                   = getAdminToken() ?? "";
  const [dbStats, setDbStats]   = useState<Record<string, number>>({});
  const [lookups, setLookups]   = useState<Record<string, unknown[]>>({});
  const [running, setRunning]   = useState<string | null>(null);
  const [showForm, setShowForm] = useState(false);
  const [form, setForm]         = useState({
    full_name: "", agent_id: "", password: "", phone: "",
    state_id: "", market_id: "", role: "Reporter",
  });

  useEffect(() => {
    async function load() {
      const [stats, lk] = await Promise.all([
        getDbStats(token), getLookups(token),
      ]);
      if (stats.data) setDbStats(stats.data as Record<string, number>);
      if (lk.data) setLookups(lk.data as Record<string, unknown[]>);
    }
    load();
  }, [token]);

  async function runStage(stage: "cleaning" | "forecasting" | "optimization" | "full") {
    setRunning(stage);
    let res;
    if (stage === "cleaning")      res = await triggerCleaning(token);
    else if (stage === "forecasting") res = await triggerForecasting(token);
    else if (stage === "optimization") res = await triggerOptimization(token);
    else res = await triggerPipeline(token);

    if (res.error) toast.error(`Failed: ${res.error}`);
    else toast.success(`${stage === "full" ? "Full pipeline" : stage} started — check logs for status.`);
    setRunning(null);
  }

  async function handleCreateAgent(e: React.FormEvent) {
    e.preventDefault();
    if (!form.full_name || !form.agent_id || !form.password || !form.state_id) {
      toast.error("Please fill all required fields.");
      return;
    }
    const res = await createAgent(token, {
      ...form,
      state_id:  parseInt(form.state_id),
      market_id: form.market_id ? parseInt(form.market_id) : null,
    });
    if (res.error) { toast.error(res.error); return; }
    toast.success(`Agent ${form.agent_id} created successfully.`);
    setForm({ full_name: "", agent_id: "", password: "", phone: "", state_id: "", market_id: "", role: "Reporter" });
    setShowForm(false);
  }

  const states  = (lookups.states  ?? []) as Array<{id: number; name: string}>;
  const markets = (lookups.markets ?? []) as Array<{id: number; name: string; state_id: number}>;
  const filteredMarkets = form.state_id
    ? markets.filter(m => m.state_id === parseInt(form.state_id))
    : markets;

  const STAGES = [
    { key: "cleaning",     label: "Data Cleaning",  icon: Database,  desc: "Process raw Kobo submissions through Z-score outlier detection and write to cleaned_prices." },
    { key: "forecasting",  label: "Forecasting",    icon: BarChart2, desc: "Train Prophet models on cleaned data and generate 7-day price forecasts for all 32 state-commodity pairs." },
    { key: "optimization", label: "Optimization",   icon: Zap,       desc: "Run PuLP linear programming on latest forecasts to generate ranked weekly trade route recommendations." },
  ] as const;

  return (
    <div className="p-6 space-y-6 page-enter">
      <Toaster position="top-right" />
      <div>
        <h1 className="text-2xl font-heading font-bold text-gray-900">Data & Pipeline</h1>
        <p className="text-sm text-gray-500 mt-0.5">Manage agents, run pipeline stages, inspect database</p>
      </div>

      {/* Full pipeline button */}
      <div className="bg-green-dark rounded-xl p-5 flex items-center justify-between">
        <div>
          <p className="text-white font-heading font-semibold">Run Full Pipeline</p>
          <p className="text-white/50 text-xs mt-0.5">Cleaning → Forecasting → Optimization in sequence</p>
        </div>
        <button
          onClick={() => runStage("full")}
          disabled={!!running}
          className="bg-white text-green font-semibold text-sm px-5 py-2.5 rounded-lg
                     hover:bg-green-faint transition-colors disabled:opacity-50 flex items-center gap-2"
        >
          {running === "full"
            ? <><Loader2 size={15} className="animate-spin" /> Running…</>
            : <><Play size={15} /> Run Now</>
          }
        </button>
      </div>

      {/* Individual stages */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
        {STAGES.map(({ key, label, icon: Icon, desc }) => (
          <div key={key} className="bg-white rounded-xl border border-gray-100 shadow-card p-5">
            <div className="flex items-center gap-3 mb-3">
              <div className="w-9 h-9 rounded-lg bg-green-faint flex items-center justify-center">
                <Icon size={18} className="text-green" />
              </div>
              <p className="font-heading font-semibold text-gray-800 text-sm">{label}</p>
            </div>
            <p className="text-xs text-gray-500 mb-4 leading-relaxed">{desc}</p>
            <button
              onClick={() => runStage(key)}
              disabled={!!running}
              className="btn-secondary w-full text-xs py-2"
            >
              {running === key
                ? <><Loader2 size={13} className="animate-spin" /> Running…</>
                : <><Play size={13} /> Run {label}</>
              }
            </button>
          </div>
        ))}
      </div>

      {/* Register agent */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-card overflow-hidden">
        <button
          onClick={() => setShowForm(!showForm)}
          className="w-full px-6 py-4 flex items-center justify-between hover:bg-gray-50/50 transition-colors border-b border-gray-100"
        >
          <span className="font-heading font-semibold text-gray-800 text-sm">Register New Agent</span>
          <span className="text-xs text-gray-400">{showForm ? "▲ Collapse" : "▼ Expand"}</span>
        </button>

        {showForm && (
          <form onSubmit={handleCreateAgent} className="p-6 grid grid-cols-1 md:grid-cols-2 gap-4">
            {[
              { key: "full_name", label: "Full Name *", placeholder: "Emeka Okafor" },
              { key: "agent_id",  label: "Agent ID *",  placeholder: "TFN-OY-001" },
              { key: "password",  label: "Password *",  placeholder: "Temporary password" },
              { key: "phone",     label: "Phone",       placeholder: "08012345678" },
            ].map(f => (
              <div key={f.key}>
                <label className="form-label">{f.label}</label>
                <input
                  className="form-input"
                  placeholder={f.placeholder}
                  value={(form as Record<string, string>)[f.key]}
                  onChange={e => setForm(prev => ({ ...prev, [f.key]: e.target.value }))}
                />
              </div>
            ))}

            <div>
              <label className="form-label">State *</label>
              <select
                className="form-select"
                value={form.state_id}
                onChange={e => setForm(prev => ({ ...prev, state_id: e.target.value, market_id: "" }))}
              >
                <option value="">Select state…</option>
                {states.map(s => <option key={s.id} value={s.id}>{s.name}</option>)}
              </select>
            </div>

            <div>
              <label className="form-label">Market</label>
              <select
                className="form-select"
                value={form.market_id}
                onChange={e => setForm(prev => ({ ...prev, market_id: e.target.value }))}
                disabled={!form.state_id}
              >
                <option value="">Select market…</option>
                {filteredMarkets.map((m: {id: number; name: string}) => (
                  <option key={m.id} value={m.id}>{m.name}</option>
                ))}
              </select>
            </div>

            <div className="md:col-span-2 flex gap-3">
              <button type="submit" className="btn-primary">
                <CheckCircle size={15} /> Create Agent
              </button>
              <button type="button" onClick={() => setShowForm(false)} className="btn-ghost">
                Cancel
              </button>
            </div>
          </form>
        )}
      </div>

      {/* DB Stats */}
      <div className="bg-white rounded-xl border border-gray-100 shadow-card p-6">
        <h2 className="text-base font-heading font-semibold text-gray-800 mb-4">Database</h2>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
          {Object.entries(dbStats).map(([table, count]) => (
            <div key={table} className="bg-gray-50 rounded-lg p-3">
              <p className="text-xs text-gray-400 mb-1 truncate">{table}</p>
              <p className="text-lg font-heading font-bold text-gray-800">
                {count === -1 ? "—" : count.toLocaleString()}
              </p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
