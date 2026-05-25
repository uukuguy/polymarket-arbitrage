// /candidates — L2 active candidate set (Phase 03 Plan 03-08, D-07).
//
// Reads l2_candidates WHERE removed_at_ts IS NULL via anon key + RLS.
// Server Component (no 'use client') — initial load is server-rendered.
// Fail-soft (LEARNINGS P5): Supabase down → banner + empty rows, NOT 500.
import { getActiveCandidates, type L2Candidate } from "@/lib/supabase/l2-queries";

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

function fmtLiquidity(ranking: Record<string, unknown> | null): string {
  if (!ranking) return "-";
  const v = (ranking as { liquidity?: number | string }).liquidity;
  if (v === undefined || v === null) return "-";
  const n = typeof v === "number" ? v : parseFloat(String(v));
  if (Number.isNaN(n)) return "-";
  return `$${Math.round(n).toLocaleString()}`;
}

function truncateAssetId(asset_id: string): string {
  if (!asset_id) return "-";
  if (asset_id.length <= 16) return asset_id;
  return `${asset_id.slice(0, 6)}…${asset_id.slice(-6)}`;
}

export default async function CandidatesPage() {
  let rows: L2Candidate[] = [];
  let errorMsg: string | null = null;

  try {
    rows = await getActiveCandidates(100);
  } catch (e) {
    errorMsg = e instanceof Error ? e.message : "Supabase unreachable";
  }

  return (
    <main style={{ padding: 24 }}>
      <h1 style={{ fontSize: 24, marginBottom: 8 }}>L2 candidates (active)</h1>
      <p style={{ fontSize: 13, color: "#888", marginBottom: 16 }}>
        Source: <code>l2_candidates</code> WHERE <code>removed_at_ts IS NULL</code>{" "}
        (Alembic 003). Recipe-driven ∪ watchlist YAML; refreshed on each L1
        snapshot.complete NOTIFY (60s debounce, 500 cap).
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
          Supabase warning: {errorMsg}. Showing empty list (fail-soft).
        </div>
      )}
      {rows.length === 0 && !errorMsg && (
        <p style={{ color: "#888", fontSize: 13 }}>
          No active candidates yet. Wait for next L1 snapshot tick (cron 6h) or
          L2 bootstrap pass.
        </p>
      )}
      {rows.length > 0 && (
        <table style={{ width: "100%", fontSize: 13, borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #333", textAlign: "left" }}>
              <th style={{ padding: "8px 6px" }}>asset_id</th>
              <th style={{ padding: "8px 6px" }}>recipe</th>
              <th style={{ padding: "8px 6px" }}>liquidity</th>
              <th style={{ padding: "8px 6px" }}>included_at (UTC)</th>
              <th style={{ padding: "8px 6px" }}>source</th>
              <th style={{ padding: "8px 6px" }}>drill</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.id} style={{ borderBottom: "1px solid #1d1d1d" }}>
                <td
                  style={{
                    padding: "6px 6px",
                    fontFamily: "ui-monospace, SFMono-Regular, monospace",
                  }}
                  title={row.asset_id}
                >
                  {truncateAssetId(row.asset_id)}
                </td>
                <td style={{ padding: "6px 6px" }}>{row.recipe_name}</td>
                <td style={{ padding: "6px 6px" }}>
                  {fmtLiquidity(row.ranking_score)}
                </td>
                <td style={{ padding: "6px 6px" }}>
                  {fmtTimestamp(row.included_at_ts)}
                </td>
                <td style={{ padding: "6px 6px", color: "#888" }}>{row.source}</td>
                <td style={{ padding: "6px 6px", fontSize: 12 }}>
                  <a
                    href={`/asset/${encodeURIComponent(row.asset_id)}/tob`}
                    style={{ color: "#7fc6ff" }}
                  >
                    tob
                  </a>{" "}
                  ·{" "}
                  <a
                    href={`/asset/${encodeURIComponent(row.asset_id)}/trades`}
                    style={{ color: "#7fc6ff" }}
                  >
                    trades
                  </a>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
