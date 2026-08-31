// Root layout — Polymarket Arbitrage L1 Dashboard.
// Dark mode by default (D-18 read-only timeline UX).
import type { Metadata } from "next";
import type { ReactNode } from "react";

export const metadata: Metadata = {
  title: "Polymarket Arbitrage L1 Dashboard",
  description: "Read-only L1 snapshot timeline + Top movers + scan trigger",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" className="dark">
      <body
        style={{
          background: "#0a0a0a",
          color: "#e5e5e5",
          fontFamily: "ui-sans-serif, system-ui, sans-serif",
          margin: 0,
          minHeight: "100vh",
        }}
      >
        <nav
          style={{
            padding: "12px 24px",
            borderBottom: "1px solid #222",
            display: "flex",
            gap: 16,
            fontSize: 14,
          }}
        >
          <strong>polyarb M1</strong>
          <a href="/business" style={{ color: "#9ec5fe" }}>Business Research</a>
          <a href="/control-plane" style={{ color: "#9ec5fe" }}>Runtime</a>
        </nav>
        {children}
      </body>
    </html>
  );
}
