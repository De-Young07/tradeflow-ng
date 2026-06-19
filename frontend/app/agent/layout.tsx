"use client";

import { usePathname, useRouter } from "next/navigation";
import { ClipboardList, CheckSquare, MessageCircle, LogOut } from "lucide-react";
import clsx from "clsx";

const TABS = [
  { href: "/agent",        label: "My Trades",    icon: ClipboardList },
  { href: "/agent/report", label: "Report",       icon: CheckSquare },
  { href: "/agent/submit", label: "Submit Price", icon: MessageCircle },
];

export default function AgentLayout({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router   = useRouter();

  async function handleLogout() {
    await fetch("/api/auth/agent", { method: "DELETE" });
    router.push("/login/agent");
  }

  return (
    <div
      className="min-h-screen flex flex-col"
      style={{ background: "linear-gradient(160deg, #0D1F14 0%, #1A3328 100%)" }}
    >
      {/* Top bar */}
      <header className="flex items-center justify-between px-5 pt-5 pb-2">
        <div className="flex items-center gap-2">
          <span className="text-xl">🌾</span>
          <span className="text-white font-heading font-bold text-base">TradeFlow NG</span>
        </div>
        <button
          onClick={handleLogout}
          className="flex items-center gap-1.5 text-white/50 hover:text-white/80 text-xs transition-colors"
        >
          <LogOut size={14} /> Sign out
        </button>
      </header>

      {/* Scrollable content */}
      <main className="flex-1 overflow-auto pb-24">
        {children}
      </main>

      {/* Bottom tab bar */}
      <nav
        className="fixed bottom-0 left-0 right-0 z-50 border-t border-white/10"
        style={{ background: "#0D1F14", paddingBottom: "env(safe-area-inset-bottom)" }}
      >
        <div className="flex">
          {TABS.map(({ href, label, icon: Icon }) => {
            const active = href === "/agent"
              ? pathname === "/agent"
              : pathname.startsWith(href);
            return (
              <a
                key={href}
                href={href}
                className={clsx(
                  "flex-1 flex flex-col items-center justify-center py-3 gap-1 text-xs font-medium transition-colors",
                  active ? "text-gold" : "text-white/40 hover:text-white/70"
                )}
              >
                <Icon size={20} />
                <span>{label}</span>
                {active && (
                  <div className="absolute bottom-0 h-0.5 w-10 rounded-full bg-gold" />
                )}
              </a>
            );
          })}
        </div>
      </nav>
    </div>
  );
}
