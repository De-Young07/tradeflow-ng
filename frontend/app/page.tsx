"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { Eye, EyeOff, Loader2 } from "lucide-react";
import { agentLogin } from "@/lib/api";
import { saveAgentToken } from "@/lib/auth";

export default function AgentLoginPage() {
  const router = useRouter();
  const [agentId, setAgentId] = useState("");
  const [password, setPassword] = useState("");
  const [showPass, setShowPass] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    if (!agentId || !password) {
      setError("Please enter your Agent ID and password.");
      return;
    }
    setLoading(true);
    setError("");

    const res = await agentLogin(agentId.trim().toUpperCase(), password.trim());

    const token     = res.access_token;
    const agentData = res.agent_data;

    if (res.error || !token) {
      setError(res.error || "Incorrect Agent ID or password.");
      setLoading(false);
      return;
    }

    // Persist token so API calls can send it as a Bearer token.
    saveAgentToken(token, agentData ?? {});

    await fetch("/api/auth/agent", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        token,
        agentData,
      }),
    });

    router.push("/agent");
  }

  return (
    <div
      className="min-h-screen flex items-center justify-center px-4 relative overflow-hidden"
      style={{ background: "linear-gradient(160deg, #0D1F14 0%, #1A3328 60%, #0D2B1C 100%)" }}
    >
      {/* Geometric circle accents */}
      {[
        { size: 400, top: "-100px", right: "-100px", opacity: 0.06 },
        { size: 250, bottom: "-80px", left: "-80px",  opacity: 0.05 },
        { size: 150, top: "30%",     left: "5%",      opacity: 0.04 },
      ].map((c, i) => (
        <div
          key={i}
          className="absolute rounded-full border pointer-events-none"
          style={{
            width: c.size, height: c.size,
            top: c.top, right: c.right,
            bottom: c.bottom, left: c.left,
            borderColor: `rgba(200, 134, 10, ${c.opacity * 3})`,
            background: `radial-gradient(circle, rgba(26,107,60,${c.opacity}), transparent)`,
          }}
        />
      ))}

      <div className="relative w-full max-w-sm">
        {/* Logo area */}
        <div className="text-center mb-8">
          <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl mb-4"
               style={{ background: "rgba(26, 107, 60, 0.3)", border: "1px solid rgba(26,107,60,0.4)" }}>
            <span className="text-3xl">🌾</span>
          </div>
          <h1 className="text-2xl font-heading font-bold text-white">TradeFlow NG</h1>
          <p className="text-sm text-white/50 mt-1">Field Agent Portal</p>
        </div>

        {/* Card */}
        <div className="bg-white rounded-2xl shadow-2xl overflow-hidden">
          <div className="h-1" style={{ background: "linear-gradient(90deg, #1A6B3C, #C8860A)" }} />

          <div className="p-7">
            <h2 className="text-base font-heading font-semibold text-gray-800 mb-1">
              Agent Login
            </h2>
            <p className="text-sm text-gray-500 mb-5">
              Enter your Agent ID and password to continue.
            </p>

            {error && (
              <div className="mb-4 px-3 py-2.5 rounded-lg bg-red-50 border border-red-200
                              text-sm text-red-700">
                {error}
              </div>
            )}

            <form onSubmit={handleLogin} className="space-y-4">
              <div>
                <label className="form-label">Agent ID</label>
                <input
                  type="text"
                  value={agentId}
                  onChange={e => setAgentId(e.target.value.toUpperCase())}
                  className="form-input font-mono tracking-wider"
                  placeholder="TFN-KW-001"
                  autoComplete="username"
                  autoFocus
                />
                <p className="text-xs text-gray-400 mt-1">
                  Contact your supervisor if you don&apos;t know your ID.
                </p>
              </div>

              <div>
                <label className="form-label">Password</label>
                <div className="relative">
                  <input
                    type={showPass ? "text" : "password"}
                    value={password}
                    onChange={e => setPassword(e.target.value)}
                    className="form-input pr-10"
                    placeholder="Your password"
                    autoComplete="current-password"
                  />
                  <button
                    type="button"
                    onClick={() => setShowPass(!showPass)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400
                               hover:text-gray-600"
                  >
                    {showPass ? <EyeOff size={16} /> : <Eye size={16} />}
                  </button>
                </div>
              </div>

              <button
                type="submit"
                disabled={loading}
                className="btn-primary w-full h-12 text-base mt-1"
              >
                {loading
                  ? <><Loader2 size={16} className="animate-spin" /> Signing in…</>
                  : "Login →"
                }
              </button>
            </form>
          </div>

          <div className="px-7 py-3.5 bg-gray-50 border-t border-gray-100">
            <p className="text-xs text-center text-gray-400">
              Having trouble? Contact your TradeFlow NG supervisor.
            </p>
          </div>
        </div>
      </div>
    </div>
  );
}
