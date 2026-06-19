import type { Metadata } from "next";
import { Plus_Jakarta_Sans, Inter } from "next/font/google";
import "../styles/globals.css";

const jakarta = Plus_Jakarta_Sans({
  subsets: ["latin"],
  variable: "--font-jakarta",
  weight: ["400", "500", "600", "700", "800"],
  display: "swap",
});

const inter = Inter({
  subsets: ["latin"],
  variable: "--font-inter",
  weight: ["400", "500", "600"],
  display: "swap",
});

export const metadata: Metadata = {
  title: { default: "TradeFlow NG", template: "%s | TradeFlow NG" },
  description: "AI-powered agricultural trade intelligence for Nigeria. Flowing Trade. Feeding Nigeria.",
  icons: { icon: "/favicon.ico" },
  openGraph: {
    title: "TradeFlow NG",
    description: "Knowing where to sell before you leave.",
    siteName: "TradeFlow NG",
  },
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className={`${jakarta.variable} ${inter.variable}`}>
      <body className="font-body antialiased">{children}</body>
    </html>
  );
}
