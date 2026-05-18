// /movers — top informationally-interesting markets from Supabase view top_movers_view.
//
// Architectural note (deviation from plan's BLOCKER-5 spec):
// Plan 02-06 originally specified a cross-snapshot mid_price diff view requiring a
// `markets` table with snapshot_id history. Actual shipped schema (Plan 03 Alembic 001)
// is `markets_latest` (full-overwrite per push), so no historical join is possible.
// Alembic 002 (Plan 02-08) instead built top_movers_view as a proximity-to-0.5 proxy
// ("most uncertain" markets). True cross-snapshot delta is deferred to Phase 02.1
// (requires a markets_history sister table — see Alembic 002 docstring).
//
// Fail-soft (LEARNINGS P5): Supabase down -> banner + empty, NOT 500.
import { getServerSupabase } from "@/lib/supabase";
import type { TopMoverRow } from "@/lib/types";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function MoversPage() {
  let rows: TopMoverRow[] = [];
  let errorMsg: string | null = null;

  try {
    const supabase = await getServerSupabase();
    const { data, error } = await supabase
      .from("top_movers_view")
      .select(
        "market_id, question, question_zh, slug, event_slug, mid_price, liquidity_usd, volume_usd, end_time_ms, snapshot_id, uncertainty_score",
      )
      .order("uncertainty_score", { ascending: true })
      .limit(50);
    if (error) {
      errorMsg = error.message;
    } else {
      rows = (data ?? []) as TopMoverRow[];
    }
  } catch (e) {
    errorMsg = e instanceof Error ? e.message : "Supabase unreachable";
  }

  return (
    <main style={{ padding: 24 }}>
      <h1 style={{ fontSize: 24, marginBottom: 8 }}>Top markets (uncertainty proxy)</h1>
      <p style={{ fontSize: 13, color: "#888", marginBottom: 16 }}>
        Source: <code>top_movers_view</code> (Alembic 002). Ranks markets by proximity
        of <code>mid_price</code> to 0.5 (smallest = most uncertain). True cross-snapshot
        price-delta deferred to Phase 02.1 (needs markets_history schema).
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
          View is empty — wait for next Fly daemon cron tick (subset 12h).
        </p>
      )}
      {rows.length > 0 && (
        <table
          style={{
            width: "100%",
            fontSize: 13,
            borderCollapse: "collapse",
          }}
        >
          <thead>
            <tr style={{ borderBottom: "1px solid #333", textAlign: "left" }}>
              <th style={{ padding: "8px 6px" }}>Market</th>
              <th style={{ padding: "8px 6px" }}>mid_price</th>
              <th style={{ padding: "8px 6px" }}>|d - 0.5|</th>
              <th style={{ padding: "8px 6px" }}>Liquidity</th>
              <th style={{ padding: "8px 6px" }}>Volume</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.market_id}
                style={{ borderBottom: "1px solid #1d1d1d" }}
              >
                <td style={{ padding: "6px 6px", maxWidth: 480 }}>
                  {row.question_zh || row.question}
                </td>
                <td style={{ padding: "6px 6px" }}>
                  {row.mid_price?.toFixed(3) ?? "-"}
                </td>
                <td style={{ padding: "6px 6px" }}>
                  {row.uncertainty_score?.toFixed(3) ?? "-"}
                </td>
                <td style={{ padding: "6px 6px" }}>
                  ${row.liquidity_usd?.toLocaleString() ?? "-"}
                </td>
                <td style={{ padding: "6px 6px" }}>
                  ${row.volume_usd?.toLocaleString() ?? "-"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
