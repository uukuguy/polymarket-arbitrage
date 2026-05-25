// /signals — L2 signals surface (Phase 03 Plan 03-08, D-07).
//
// Phase 03 status: SCHEMA-ONLY placeholder.
// The l2_signals table exists (Alembic 003) but no Phase 03 code writes to it.
// M4 strategies (Phase 04+) populate it — this page therefore renders an
// empty state with a clear "Phase 04 will populate" message.
//
// Server Component; anon key + RLS reads only.
// Fail-soft (LEARNINGS P5): Supabase error → banner, NOT 500.
import { getSignals, type L2Signal } from "@/lib/supabase/l2-queries";

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

function severityColor(sev: string): string {
  const s = (sev || "").toLowerCase();
  if (s === "critical" || s === "fatal") return "rgba(180, 30, 30, 0.30)";
  if (s === "warning" || s === "warn") return "rgba(180, 130, 0, 0.25)";
  return "transparent";
}

export default async function SignalsPage() {
  let rows: L2Signal[] = [];
  let errorMsg: string | null = null;

  try {
    rows = await getSignals(24, 100);
  } catch (e) {
    errorMsg = e instanceof Error ? e.message : "Supabase unreachable";
  }

  return (
    <main style={{ padding: 24 }}>
      <h1 style={{ fontSize: 24, marginBottom: 8 }}>L2 signals</h1>
      <p style={{ fontSize: 13, color: "#888", marginBottom: 16 }}>
        Source: <code>l2_signals</code> (Alembic 003). Phase 03 leaves this
        table empty intentionally — M4 strategies will write here in Phase 04+.
        This page reserves the dashboard slot so URL + nav don&apos;t break later.
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
          Supabase warning: {errorMsg}. (fail-soft)
        </div>
      )}
      {rows.length === 0 && !errorMsg && (
        <div
          style={{
            background: "#161b22",
            border: "1px dashed #2d333b",
            padding: 24,
            borderRadius: 6,
            textAlign: "center",
            color: "#888",
            fontSize: 14,
          }}
        >
          <p style={{ marginBottom: 8 }}>
            No signals yet — Phase 04 will populate this surface.
          </p>
          <p style={{ fontSize: 12, color: "#666" }}>
            Phase 03 only set up the table schema (5-table Alembic 003). The
            strategies that emit signals (M4 smart-strategies workstream) are
            not yet wired.
          </p>
        </div>
      )}
      {rows.length > 0 && (
        <table style={{ width: "100%", fontSize: 13, borderCollapse: "collapse" }}>
          <thead>
            <tr style={{ borderBottom: "1px solid #333", textAlign: "left" }}>
              <th style={{ padding: "8px 6px" }}>ts (UTC)</th>
              <th style={{ padding: "8px 6px" }}>asset_id</th>
              <th style={{ padding: "8px 6px" }}>signal_type</th>
              <th style={{ padding: "8px 6px" }}>severity</th>
              <th style={{ padding: "8px 6px" }}>ack?</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr
                key={row.id}
                style={{
                  background: severityColor(row.severity),
                  borderBottom: "1px solid #1d1d1d",
                }}
              >
                <td style={{ padding: "6px 6px" }}>{fmtTimestamp(row.ts)}</td>
                <td
                  style={{
                    padding: "6px 6px",
                    fontFamily: "ui-monospace, SFMono-Regular, monospace",
                  }}
                  title={row.asset_id}
                >
                  {row.asset_id.slice(0, 12)}…
                </td>
                <td style={{ padding: "6px 6px" }}>{row.signal_type}</td>
                <td style={{ padding: "6px 6px" }}>{row.severity}</td>
                <td style={{ padding: "6px 6px", color: "#888" }}>
                  {row.acknowledged_at ? "✓" : "-"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </main>
  );
}
