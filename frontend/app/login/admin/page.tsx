"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { Eye, EyeOff, Loader2 } from "lucide-react";
import { adminLogin } from "@/lib/api";

export default function AdminLoginPage() {
  const router = useRouter();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [showPass, setShowPass] = useState(false);
  const [loading, setLoading]   = useState(false);
  const [error, setError]       = useState("");

  async function handleLogin(e: React.FormEvent) {
    e.preventDefault();
    if (!username || !password) {
      setError("Please enter both username and password.");
      return;
    }
    setLoading(true);
    setError("");

    const res = await adminLogin(username.trim(), password.trim());

    if (res.error || !res.data?.access_token) {
      setError(res.error || "Login failed. Please try again.");
      setLoading(false);
      return;
    }

    await fetch("/api/auth/admin", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ token: res.data.access_token }),
    });

    router.push("/admin");
  }

  return (
    <div
      className="min-h-screen flex items-center justify-center px-4"
      style={{ background: "linear-gradient(135deg, #0D1F14 0%, #1A3328 100%)" }}
    >
      {/* Subtle radial accents */}
      <div className="absolute inset-0 overflow-hidden pointer-events-none">
        <div className="absolute -top-40 -right-40 w-96 h-96 rounded-full opacity-10"
             style={{ background: "radial-gradient(circle, #1A6B3C, transparent)" }} />
        <div className="absolute -bottom-40 -left-40 w-80 h-80 rounded-full opacity-10"
             style={{ background: "radial-gradient(circle, #C8860A, transparent)" }} />
      </div>

      <div className="relative w-full max-w-md">
        <div className="bg-white rounded-2xl shadow-2xl overflow-hidden">
          {/* Gradient top bar */}
          <div className="h-1.5 w-full"
               style={{ background: "linear-gradient(90deg, #1A6B3C, #C8860A)" }} />

          <div className="p-8">
            {/* Logo */}
            <div className="flex flex-col items-center mb-8">
              <img
                src="/logo-icon.png"
                alt="TradeFlow NG"
                className="w-16 h-16 object-contain mb-3"
                onError={(e) => {
                  // Fallback if logo not found
                  (e.target as HTMLImageElement).style.display = "none";
                }}
              />
              <img
                src="/logo-full.png"
                alt="TradeFlow NG"
                className="h-8 object-contain"
                onError={(e) => {
                  const el = e.target as HTMLImageElement;
                  el.style.display = "none";
                  el.insertAdjacentHTML("afterend",
                    '<p class="text-lg font-bold text-gray-900" style="font-family:sans-serif">TradeFlow NG</p>'
                  );
                }}
              />
              <p className="text-xs text-gray-400 mt-1.5">Admin Control Dashboard</p>
            </div>

            <h2 className="text-lg font-semibold text-gray-800 mb-1"
                style={{ fontFamily: "var(--font-jakarta, sans-serif)" }}>
              Welcome back
            </h2>
            <p className="text-sm text-gray-500 mb-6">
              Sign in to access the admin dashboard.
            </p>

            {error && (
              <div className="mb-4 px-4 py-3 rounded-lg bg-red-50 border border-red-200 text-sm text-red-700 flex items-center gap-2">
                <span>⚠</span> {error}
              </div>
            )}

            <form onSubmit={handleLogin} className="space-y-4">
              <div>
                <label className="form-label">Username</label>
                <input
                  type="text"
                  value={username}
                  onChange={e => setUsername(e.target.value)}
                  className="form-input"
                  placeholder="admin"
                  autoComplete="username"
                  autoFocus
                />
              </div>

              <div>
                <label className="form-label">Password</label>
                <div className="relative">
                  <input
                    type={showPass ? "text" : "password"}
                    value={password}
                    onChange={e => setPassword(e.target.value)}
                    className="form-input pr-10"
                    placeholder="••••••••"
                    autoComplete="current-password"
                  />
                  <button
                    type="button"
                    onClick={() => setShowPass(!showPass)}
                    className="absolute right-3 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600 transition-colors"
                  >
                    {showPass ? <EyeOff size={16} /> : <Eye size={16} />}
                  </button>
                </div>
              </div>

              <button
                type="submit"
                disabled={loading}
                className="btn-primary w-full mt-2 h-11 text-base"
              >
                {loading
                  ? <><Loader2 size={16} className="animate-spin" /> Signing in…</>
                  : "Login →"
                }
              </button>
            </form>
          </div>

          <div className="px-8 py-4 bg-gray-50 border-t border-gray-100">
            <p className="text-xs text-center text-gray-400">
              Flowing Trade. Feeding Nigeria.
            </p>
          </div>
        </div>

        <p className="text-center text-xs text-white/30 mt-6">
          Contact your system administrator if you need access.
        </p>
      </div>
    </div>
  );
}
