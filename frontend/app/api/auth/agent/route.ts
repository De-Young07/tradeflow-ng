import { NextRequest, NextResponse } from "next/server";
import { AGENT_COOKIE } from "@/lib/auth";

export async function POST(request: NextRequest) {
  const { token, agentData } = await request.json();
  const res = NextResponse.json({ ok: true, agentData });
  res.cookies.set(AGENT_COOKIE, token, {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax",
    maxAge: 60 * 60 * 24,
    path: "/",
  });
  return res;
}

export async function DELETE() {
  const res = NextResponse.json({ ok: true });
  res.cookies.delete(AGENT_COOKIE);
  return res;
}
