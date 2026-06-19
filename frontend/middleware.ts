/**
 * TradeFlow NG — Route Protection Middleware
 */

import { NextRequest, NextResponse } from "next/server";
import { ADMIN_COOKIE, AGENT_COOKIE, verifyJWT } from "./lib/auth";

export async function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Admin routes
  if (pathname.startsWith("/admin")) {
    const token = request.cookies.get(ADMIN_COOKIE)?.value;
    if (!token) {
      return NextResponse.redirect(new URL("/login/admin", request.url));
    }
    const payload = await verifyJWT(token);
    if (!payload || payload.role !== "admin") {
      const res = NextResponse.redirect(new URL("/login/admin", request.url));
      res.cookies.delete(ADMIN_COOKIE);
      return res;
    }
  }

  // Agent routes
  if (pathname.startsWith("/agent")) {
    const token = request.cookies.get(AGENT_COOKIE)?.value;
    if (!token) {
      return NextResponse.redirect(new URL("/login/agent", request.url));
    }
    const payload = await verifyJWT(token);
    if (!payload || payload.role !== "agent") {
      const res = NextResponse.redirect(new URL("/login/agent", request.url));
      res.cookies.delete(AGENT_COOKIE);
      return res;
    }
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/admin/:path*", "/agent/:path*"],
};
