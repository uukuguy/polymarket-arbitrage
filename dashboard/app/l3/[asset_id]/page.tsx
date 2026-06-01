// /l3/[asset_id] — L3 detail page (Phase 05 Plan 05-05).
//
// Server Component (no 'use client') — initial render is fully server-rendered.
// Reads l2_ohlc_1m (last 24h) + l2_book_levels (latest top-20) via anon RLS
// (Alembic 005). KlineChart is the only client subcomponent; lightweight-charts
// is dynamically imported INSIDE useEffect so SSR cannot crash on `window`.
//
// Fail-soft contract (Phase 02 LEARNINGS P5): Supabase down → banner + empty
// arrays → KlineChart renders empty canvas + DepthLadder shows 10 blank rows.
// NOT a 500 page.
import {
  getOhlcForAsset,
  getBookLevelsLatest,
  type L2OhlcRow,
  type L2BookLevel,
} from "@/lib/supabase/l2-queries";
import KlineChart from "./KlineChart";
import DepthLadder from "./DepthLadder";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function L3Page({
  params,
}: {
  params: Promise<{ asset_id: string }>;
}) {
  const { asset_id } = await params;
  const assetId = decodeURIComponent(asset_id);

  let ohlc: L2OhlcRow[] = [];
  let ladder: L2BookLevel[] = [];
  let errorMsg: string | null = null;
  try {
    [ohlc, ladder] = await Promise.all([
      getOhlcForAsset(assetId, "1m", 24),
      getBookLevelsLatest(assetId),
    ]);
  } catch (e) {
    errorMsg = e instanceof Error ? e.message : "Supabase unreachable";
  }

  return (
    <main
      style={{
        padding: 24,
        fontFamily: "ui-monospace, SFMono-Regular, monospace",
        color: "#ddd",
        background: "#0a0a0a",
        minHeight: "100vh",
      }}
    >
      <h1 style={{ fontSize: 22, marginBottom: 8 }}>
        L3 — {assetId.slice(0, 18)}…
      </h1>
      <div style={{ fontSize: 12, color: "#888", marginBottom: 16 }}>
        OHLC 1m (last 24h) · depth top-10 per side · <code>l2_book_levels</code>{" "}
        live · asset_id: <span title={assetId}>{assetId}</span>
      </div>
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
          Supabase warning: {errorMsg}. Showing empty data (fail-soft).
        </div>
      )}
      {!errorMsg && ohlc.length === 0 && ladder.length === 0 && (
        <div
          style={{
            background: "#1a1a1a",
            border: "1px solid #2a2a2a",
            padding: 12,
            borderRadius: 4,
            marginBottom: 16,
            fontSize: 13,
            color: "#888",
          }}
        >
          No L3 data yet for this asset — promoter may not have subscribed it,
          or no top-of-book activity in the last 24h.
        </div>
      )}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 320px",
          gap: 16,
        }}
      >
        <KlineChart ohlc={ohlc} />
        <DepthLadder rows={ladder} />
      </div>
    </main>
  );
}
