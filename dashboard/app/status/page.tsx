// /status — L1 timeline (latest 20 snapshots).
// Server Component; reads Supabase via anon-key + RLS SELECT.
// Fail-soft (LEARNINGS P5): if Supabase unreachable, show banner + empty table, NOT 500.
import { getServerSupabase } from "@/lib/supabase";
import type { Snapshot } from "@/lib/types";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function fmtTimestamp(ms: number | null | undefined): string {
  if (!ms) return "-";
  try {
    return new Date(ms).toISOString().replace("T", " ").slice(0, 19);
  } catch {
    return String(ms);
  }
}

function statusColor(status: string): string {
  const s = (status || "").toLowerCase();
  if (s === "ok" || s === "pass") return "transparent";
  if (s === "degraded" || s === "warn") return "rgba(180, 130, 0, 0.25)";
  return "rgba(180, 30, 30, 0.30)";
}

export default async function StatusPage() {
  let snapshots: Snapshot[] = [];
  let errorMsg: string | null = null;

  try {
    const supabase = await getServerSupabase();
    const { data, error } = await supabase
      .from("snapshots")
      .select(
        "id, taken_at_ms, finished_at_ms, mode, status, market_count, parquet_url, parquet_r2_url, supabase_mirror_at_ms, is_valid",
      )
      .order("taken_at_ms", { ascending: false })
      .limit(20);
    if (error) {
      errorMsg = error.message;
    } else {
      snapshots = (data ?? []) as Snapshot[];
    }
  } catch (e) {
    errorMsg = e instanceof Error ? e.message : "Supabase unreachable";
  }

  return (
    <main style={{ padding: 24 }}>
      <h1 style={{ fontSize: 24, marginBottom: 8 }}>L1 timeline (latest 20)</h1>
      <p style={{ fontSize: 13, color: "#888", marginBottom: 16 }}>
        Snapshots table from Supabase Postgres (Alembic 001 + 002). Read-only via anon-key + RLS.
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
          Supabase warning: {errorMsg}. Showing empty timeline (fail-soft).
        </div>
      )}
      {snapshots.length === 0 && !errorMsg && (
        <p style={{ color: "#888", fontSize: 13 }}>
          No snapshots yet. Fly daemon writes here after each cron tick.
        </p>
      )}
      {snapshots.length > 0 && (
        <table
          style={{
            width: "100%",
            fontSize: 13,
            borderCollapse: "collapse",
          }}
        >
          <thead>
            <tr style={{ borderBottom: "1px solid #333", textAlign: "left" }}>
              <th style={{ padding: "8px 6px" }}>taken_at (UTC)</th>
              <th style={{ padding: "8px 6px" }}>mode</th>
              <th style={{ padding: "8px 6px" }}>status</th>
              <th style={{ padding: "8px 6px" }}>markets</th>
              <th style={{ padding: "8px 6px" }}>valid</th>
              <th style={{ padding: "8px 6px" }}>mirror?</th>
              <th style={{ padding: "8px 6px" }}>r2?</th>
            </tr>
          </thead>
          <tbody>
            {snapshots.map((s) => (
              <tr
                key={s.id}
                style={{
                  background: statusColor(String(s.status)),
                  borderBottom: "1px solid #1d1d1d",
                }}
              >
                <td style={{ padding: "6px 6px" }}>{fmtTimestamp(s.taken_at_ms)}</td>
                <td style={{ padding: "6px 6px" }}>{s.mode}</td>
                <td style={{ padding: "6px 6px" }}>{s.status}</td>
                <td style={{ padding: "6px 6px" }}>{s.market_count}</td>
                <td style={{ padding: "6px 6px" }}>
                  {s.is_valid === false ? "✗" : s.is_valid === true ? "✓" : "-"}
                </td>
                <td style={{ padding: "6px 6px" }}>
                  {s.supabase_mirror_at_ms ? "✓" : "-"}
                </td>
                <td style={{ padding: "6px 6px" }}>
                  {s.parquet_r2_url ? "✓" : "-"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
