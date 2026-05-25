// /asset/[id]/trades — Trades time series for a single asset (Phase 03 Plan 03-08, D-07).
//
// Dynamic route param: asset_id (URL-decoded). Reads l2_trades last 24h.
// Server Component; anon key + RLS reads only.
// Fail-soft (LEARNINGS P5): Supabase error → banner + empty table, NOT 500.
//
// Source mix: WS-streamed (real-time) + Data API REST backfill (D-08, 7-day window).
// Phase 03 scope: NO chart / volume aggregation widget (Phase 04+).
import { getTradesForAsset, type L2Trade } from "@/lib/supabase/l2-queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function fmtTimestamp(iso: string | null | undefined): string {
  if (!iso) return "-";
  try {
    return new Date(iso).toISOString().replace("T", " ").slice(0, 19);
  } catch {
    return String(iso);
  }
}

function fmtPrice(n: number | null | undefined): string {
  if (n === null || n === undefined) return "-";
  return n.toFixed(4);
}

function fmtSize(n: number | null | undefined): string {
  if (n === null || n === undefined) return "-";
  return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
}

function fmtTaker(addr: string | null): string {
  if (!addr) return "-";
  if (addr.length <= 12) return addr;
  return `${addr.slice(0, 6)}…${addr.slice(-4)}`;
}

function sideColor(side: string | null): string {
  if (!side) return "transparent";
  const s = side.toLowerCase();
  if (s === "buy" || s === "yes") return "rgba(40, 140, 60, 0.18)";
  if (s === "sell" || s === "no") return "rgba(170, 60, 60, 0.18)";
  return "transparent";
}

export default async function AssetTradesPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const assetId = decodeURIComponent(id);

  let rows: L2Trade[] = [];
  let errorMsg: string | null = null;

  try {
    rows = await getTradesForAsset(assetId, 24, 500);
  } catch (e) {
    errorMsg = e instanceof Error ? e.message : "Supabase unreachable";
  }

  return (
    <main style={{ padding: 24 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>
        Trades — {assetId.slice(0, 12)}…
      </h1>
      <p
        style={{
          fontSize: 12,
          color: "#666",
          marginBottom: 4,
          fontFamily: "ui-monospace, SFMono-Regular, monospace",
        }}
      >
        asset_id: {assetId}
      </p>
      <p style={{ fontSize: 13, color: "#888", marginBottom: 16 }}>
        Last 24h from <code>l2_trades</code> (Alembic 003). Source mix:{" "}
        <code>ws</code> (WsConsumer real-time) + <code>data-api</code>{" "}
        (REST backfill, D-08). Dedup by <code>trade_hash UNIQUE</code>.{" "}
        <a href="/candidates" style={{ color: "#7fc6ff" }}>
          ← back to candidates
        </a>
      </p>

      {errorMsg && (
        <div
          style={{
            background: "#3b2a0a",
            border: "1px solid #6b4a10",
            padding: 12,
            borderRadius: 4,
            marginBottom: 16,
            fontSize: 13,
            color: "#ffd47a",
          }}
        >
          Supabase warning: {errorMsg}. Showing empty table (fail-soft).
        </div>
      )}
      {rows.length === 0 && !errorMsg && (
        <p style={{ color: "#888", fontSize: 13 }}>
          No trades in last 24h for this asset. Either it has not traded, or the
          backfill window has not yet covered this asset (Data API REST is
          ad-hoc, not continuous).
        </p>
      )}
      {rows.length > 0 && (
        <table style={{ width: "100%", fontSize: 13, borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #333", textAlign: "left" }}>
              <th style={{ padding: "8px 6px" }}>ts (UTC)</th>
              <th style={{ padding: "8px 6px" }}>price</th>
              <th style={{ padding: "8px 6px" }}>size</th>
              <th style={{ padding: "8px 6px" }}>side</th>
              <th style={{ padding: "8px 6px" }}>taker</th>
              <th style={{ padding: "8px 6px" }}>src</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.id}
                style={{
                  background: sideColor(row.side),
                  borderBottom: "1px solid #1d1d1d",
                }}
              >
                <td style={{ padding: "6px 6px" }}>{fmtTimestamp(row.ts)}</td>
                <td style={{ padding: "6px 6px" }}>{fmtPrice(row.price)}</td>
                <td style={{ padding: "6px 6px" }}>{fmtSize(row.size)}</td>
                <td style={{ padding: "6px 6px" }}>{row.side ?? "-"}</td>
                <td
                  style={{
                    padding: "6px 6px",
                    fontFamily: "ui-monospace, SFMono-Regular, monospace",
                    fontSize: 12,
                  }}
                  title={row.taker_address ?? ""}
                >
                  {fmtTaker(row.taker_address)}
                </td>
                <td style={{ padding: "6px 6px", color: "#888", fontSize: 11 }}>
                  {row.source}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
