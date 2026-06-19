"use client";

import { usePathname, useRouter } from "next/navigation";
import {
  LayoutDashboard, TrendingUp, Map, Table2,
  MessageSquare, Database, LogOut, Activity,
  ChevronRight,
} from "lucide-react";
import clsx from "clsx";

const NAV = [
  { href: "/admin",              label: "Overview",        icon: LayoutDashboard },
  { href: "/admin/recommendations", label: "Recommendations", icon: TrendingUp },
  { href: "/admin/forecasts",    label: "Forecasts",       icon: Activity },
  { href: "/admin/tableau",      label: "Profit Matrix",   icon: Table2 },
  { href: "/admin/feedback",     label: "Trade Feedback",  icon: MessageSquare },
  { href: "/admin/data",         label: "Data & Pipeline", icon: Database },
];

export default function AdminLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router   = useRouter();

  async function handleLogout() {
    await fetch("/api/auth/admin", { method: "DELETE" });
    router.push("/login/admin");
  }

  return (
    <div className="flex h-screen overflow-hidden bg-cream">
      {/* ── Sidebar ── */}
      <aside
        className="w-60 flex-shrink-0 flex flex-col"
        style={{ background: "#0D1F14" }}
      >
        {/* Logo */}
        <div className="px-5 py-5 border-b border-white/10">
          <div className="flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg flex items-center justify-center flex-shrink-0"
                 style={{ background: "#1A6B3C" }}>
              <span className="text-lg">🌾</span>
            </div>
            <div>
              <p className="text-sm font-heading font-bold text-white leading-none">TradeFlow NG</p>
              <p className="text-xs text-white/40 mt-0.5">Admin Panel</p>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav className="flex-1 px-3 py-4 space-y-0.5 overflow-y-auto">
          {NAV.map(({ href, label, icon: Icon }) => {
            const active = href === "/admin"
              ? pathname === "/admin"
              : pathname.startsWith(href);
            return (
              <a
                key={href}
                href={href}
                className={clsx("nav-link", active && "active")}
              >
                <Icon size={16} className="flex-shrink-0" />
                <span className="flex-1">{label}</span>
                {active && <ChevronRight size={12} className="opacity-50" />}
              </a>
            );
          })}
        </nav>

        {/* System status + logout */}
        <div className="px-3 py-4 border-t border-white/10 space-y-3">
          <div className="px-4 py-3 rounded-lg bg-white/5">
            <p className="text-xs text-white/40 mb-2 font-medium uppercase tracking-wide">
              System Status
            </p>
            <div className="space-y-1.5">
              <SystemStatus label="Forecasts" ok />
              <SystemStatus label="Optimizer" ok />
              <SystemStatus label="Database"  ok />
            </div>
          </div>
          <button
            onClick={handleLogout}
            className="nav-link w-full text-red-400/80 hover:text-red-400 hover:bg-red-500/10"
          >
            <LogOut size={16} />
            <span>Sign Out</span>
          </button>
        </div>
      </aside>

      {/* ── Main ── */}
      <main className="flex-1 overflow-auto">
        {children}
      </main>
    </div>
  );
}

function SystemStatus({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div className="flex items-center justify-between">
      <span className="text-xs text-white/60">{label}</span>
      <span className={clsx(
        "text-xs font-medium flex items-center gap-1",
        ok ? "text-green-400" : "text-red-400"
      )}>
        <span className={clsx("w-1.5 h-1.5 rounded-full", ok ? "bg-green-400" : "bg-red-400")} />
        {ok ? "Live" : "Down"}
      </span>
    </div>
  );
}
