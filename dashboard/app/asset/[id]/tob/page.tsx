// /asset/[id]/tob — Top-of-book time series for a single asset (Phase 03 Plan 03-08, D-07).
//
// Dynamic route param: asset_id (URL-decoded). Reads l2_top_of_book last 24h.
// Server Component; anon key + RLS reads only.
// Fail-soft (LEARNINGS P5): Supabase error → banner + empty table, NOT 500.
//
// Phase 03 scope: NO sparkline / chart widget (deferred to Phase 04+).
import { getTopOfBookForAsset, type L2TopOfBook } from "@/lib/supabase/l2-queries";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function fmtTimestamp(iso: string | null | undefined): string {
  if (!iso) return "-";
  try {
    return new Date(iso).toISOString().replace("T", " ").slice(11, 19);
  } catch {
    return String(iso);
  }
}

function fmtNum(n: number | null | undefined, digits = 4): string {
  if (n === null || n === undefined) return "-";
  return n.toFixed(digits);
}

function fmtUsd(n: number | null | undefined): string {
  if (n === null || n === undefined) return "-";
  return `$${Math.round(n).toLocaleString()}`;
}

export default async function AssetTobPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = await params;
  const assetId = decodeURIComponent(id);

  let rows: L2TopOfBook[] = [];
  let errorMsg: string | null = null;

  try {
    rows = await getTopOfBookForAsset(assetId, 24, 500);
  } catch (e) {
    errorMsg = e instanceof Error ? e.message : "Supabase unreachable";
  }

  return (
    <main style={{ padding: 24 }}>
      <h1 style={{ fontSize: 22, marginBottom: 4 }}>
        Top-of-book — {assetId.slice(0, 12)}…
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
        Last 24h from <code>l2_top_of_book</code> (Alembic 003, BRIN-indexed).
        WS-driven; updates as new best-bid/ask events arrive.{" "}
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
          No TOB data in last 24h for this asset. Either the asset is not (or
          was not) in the active candidate set, or the WS stream has not emitted
          a price_change yet.
        </p>
      )}
      {rows.length > 0 && (
        <table style={{ width: "100%", fontSize: 13, borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #333", textAlign: "left" }}>
              <th style={{ padding: "8px 6px" }}>ts (UTC)</th>
              <th style={{ padding: "8px 6px" }}>best_bid</th>
              <th style={{ padding: "8px 6px" }}>best_ask</th>
              <th style={{ padding: "8px 6px" }}>spread</th>
              <th style={{ padding: "8px 6px" }}>mid</th>
              <th style={{ padding: "8px 6px" }}>depth_yes</th>
              <th style={{ padding: "8px 6px" }}>depth_no</th>
              <th style={{ padding: "8px 6px" }}>src</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id} style={{ borderBottom: "1px solid #1d1d1d" }}>
                <td style={{ padding: "6px 6px" }}>{fmtTimestamp(row.ts)}</td>
                <td style={{ padding: "6px 6px" }}>{fmtNum(row.best_bid)}</td>
                <td style={{ padding: "6px 6px" }}>{fmtNum(row.best_ask)}</td>
                <td style={{ padding: "6px 6px" }}>{fmtNum(row.spread)}</td>
                <td style={{ padding: "6px 6px" }}>{fmtNum(row.mid_price)}</td>
                <td style={{ padding: "6px 6px" }}>{fmtUsd(row.depth_yes_usd)}</td>
                <td style={{ padding: "6px 6px" }}>{fmtUsd(row.depth_no_usd)}</td>
                <td style={{ padding: "6px 6px", color: "#888", fontSize: 11 }}>
                  {row.source_event ?? "-"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
