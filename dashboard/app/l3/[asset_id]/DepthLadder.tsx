// DepthLadder — server-rendered top-10 bids + top-10 asks (Phase 05 Plan 05-05).
//
// Pure presentation; no 'use client' needed. Receives up to 20 rows from
// getBookLevelsLatest (descending ts, mixed sides). We pick the most recent
// ts and group its rows by side; if the latest batch is partial (one side
// only), the other side renders blank cells — fail-soft visual contract.
import type { L2BookLevel } from "@/lib/supabase/l2-queries";

export default function DepthLadder({ rows }: { rows: L2BookLevel[] }) {
  // Pick the most recent batch (rows is desc by ts; first ts is newest).
  const latestTs = rows[0]?.ts;
  const latestBatch = latestTs ? rows.filter((r) => r.ts === latestTs) : rows;
  const bids = latestBatch
    .filter((r) => r.side === "BUY")
    .sort((a, b) => a.level - b.level);
  const asks = latestBatch
    .filter((r) => r.side === "SELL")
    .sort((a, b) => a.level - b.level);

  return (
    <div style={{ fontSize: 11 }}>
      <div style={{ marginBottom: 8, color: "#888" }}>
        Depth ladder · {latestBatch.length} rows
        {latestTs && (
          <span> · @ {new Date(latestTs).toISOString().slice(11, 19)}Z</span>
        )}
      </div>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr style={{ color: "#888" }}>
            <th style={{ textAlign: "left", padding: "2px 4px" }}>L</th>
            <th style={{ textAlign: "right", padding: "2px 4px" }}>Bid</th>
            <th style={{ textAlign: "right", padding: "2px 4px" }}>Size</th>
            <th style={{ textAlign: "right", padding: "2px 4px" }}>Ask</th>
            <th style={{ textAlign: "right", padding: "2px 4px" }}>Size</th>
          </tr>
        </thead>
        <tbody>
          {Array.from({ length: 10 }, (_, i) => {
            const b = bids[i];
            const a = asks[i];
            return (
              <tr key={i}>
                <td style={{ padding: "2px 4px", color: "#666" }}>{i + 1}</td>
                <td
                  style={{
                    padding: "2px 4px",
                    textAlign: "right",
                    color: "#26a69a",
                  }}
                >
                  {b ? b.price.toFixed(4) : ""}
                </td>
                <td style={{ padding: "2px 4px", textAlign: "right" }}>
                  {b ? b.size.toFixed(2) : ""}
                </td>
                <td
                  style={{
                    padding: "2px 4px",
                    textAlign: "right",
                    color: "#ef5350",
                  }}
                >
                  {a ? a.price.toFixed(4) : ""}
                </td>
                <td style={{ padding: "2px 4px", textAlign: "right" }}>
                  {a ? a.size.toFixed(2) : ""}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
