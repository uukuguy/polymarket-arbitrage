// L2 Supabase query helpers — Phase 03 Plan 03-08 (D-07 dashboard surface).
//
// Architecture (T-03-08-01 STRIDE Information Disclosure mitigation):
// - ONLY uses NEXT_PUBLIC_SUPABASE_ANON_KEY (via the shared `getServerSupabase`
//   helper which wraps `createServerClient(SUPABASE_URL, NEXT_PUBLIC_SUPABASE_ANON_KEY)`).
// - NEVER imports or references the privileged daemon JWT. RLS (Alembic 003)
//   grants anon SELECT on l2_* tables; the L2 daemon writes via its own
//   higher-privilege key that lives ONLY on the Fly side, never in this bundle.
// - All functions are RSC-friendly (server components only) — they call
//   getServerSupabase() which wraps next/headers cookies.
//
// Fail-soft contract (LEARNINGS P5): callers should try/catch around these
// functions and render a banner + empty rows when Supabase is unreachable.
// These helpers do NOT swallow errors — they bubble up so callers control UX.
import type { SupabaseClient } from "@supabase/supabase-js";
import { getServerSupabase } from "@/lib/supabase-server";

// ─── Row types (mirror alembic/versions/003_l2_tables.py schema) ──────────────

export interface L2Candidate {
  id: number;
  snapshot_id: number | null;
  recipe_name: string;
  asset_id: string;
  market_id: string | null;
  event_id: string | null;
  included_at_ts: string;
  removed_at_ts: string | null;
  ranking_score: Record<string, unknown> | null;
  source: string;
  // Phase 05 Plan 05-04 (D-08): non-null when L3 promoter has subscribed
  // this candidate's asset to the WS /book + /price-change firehose.
  l3_promoted_at_ts: string | null;
}

export interface L2TopOfBook {
  id: number;
  asset_id: string;
  ts: string;
  best_bid: number | null;
  best_ask: number | null;
  spread: number | null;
  mid_price: number | null;
  depth_yes_usd: number | null;
  depth_no_usd: number | null;
  source_event: string | null;
}

export interface L2Trade {
  id: number;
  asset_id: string;
  market_id: string | null;
  ts: string;
  price: number;
  size: number;
  side: string | null;
  taker_address: string | null;
  trade_hash: string | null;
  source: string;
}

export interface L2Signal {
  id: number;
  asset_id: string;
  ts: string;
  signal_type: string;
  severity: string;
  payload: Record<string, unknown> | null;
  acknowledged_by: string | null;
  acknowledged_at: string | null;
}

// ─── Query helpers ────────────────────────────────────────────────────────────

/**
 * Active candidates (removed_at_ts IS NULL), newest first.
 * Default limit=100 matches the dashboard /candidates page page size.
 */
export async function getActiveCandidates(
  limit = 100,
  supabase?: SupabaseClient,
): Promise<L2Candidate[]> {
  const client = supabase ?? (await getServerSupabase());
  const { data, error } = await client
    .from("l2_candidates")
    .select(
      "id, snapshot_id, recipe_name, asset_id, market_id, event_id, included_at_ts, removed_at_ts, ranking_score, source, l3_promoted_at_ts",
    )
    .is("removed_at_ts", null)
    .order("included_at_ts", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return (data ?? []) as L2Candidate[];
}

/**
 * Top-of-book time series for a single asset over the last `hours`.
 * Uses BRIN(ts) index from Alembic 003 — efficient even on multi-million-row tables.
 */
export async function getTopOfBookForAsset(
  assetId: string,
  hours = 24,
  limit = 1000,
  supabase?: SupabaseClient,
): Promise<L2TopOfBook[]> {
  const client = supabase ?? (await getServerSupabase());
  const cutoff = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const { data, error } = await client
    .from("l2_top_of_book")
    .select(
      "id, asset_id, ts, best_bid, best_ask, spread, mid_price, depth_yes_usd, depth_no_usd, source_event",
    )
    .eq("asset_id", assetId)
    .gte("ts", cutoff)
    .order("ts", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return (data ?? []) as L2TopOfBook[];
}

/**
 * Trades time series for a single asset over the last `hours`.
 * Backfilled by Data API client (D-08); WS appends as trades fire.
 */
export async function getTradesForAsset(
  assetId: string,
  hours = 24,
  limit = 1000,
  supabase?: SupabaseClient,
): Promise<L2Trade[]> {
  const client = supabase ?? (await getServerSupabase());
  const cutoff = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const { data, error } = await client
    .from("l2_trades")
    .select(
      "id, asset_id, market_id, ts, price, size, side, taker_address, trade_hash, source",
    )
    .eq("asset_id", assetId)
    .gte("ts", cutoff)
    .order("ts", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return (data ?? []) as L2Trade[];
}

/**
 * Signals — Phase 03 placeholder. Schema is present (Alembic 003) but
 * l2_signals table is NOT populated in Phase 03; M4 strategies (Phase 04+)
 * write to it. The /signals page therefore renders empty state until then.
 */
export async function getSignals(
  hours = 24,
  limit = 100,
  supabase?: SupabaseClient,
): Promise<L2Signal[]> {
  const client = supabase ?? (await getServerSupabase());
  const cutoff = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const { data, error } = await client
    .from("l2_signals")
    .select(
      "id, asset_id, ts, signal_type, severity, payload, acknowledged_by, acknowledged_at",
    )
    .gte("ts", cutoff)
    .order("ts", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return (data ?? []) as L2Signal[];
}

// ─── Phase 05 — L3 query helpers (Alembic 005) ────────────────────────────────
// Reads l2_book_levels table + l2_ohlc_{1m,5m,1h} views via anon RLS
// (Plan 05-01 GRANT SELECT TO anon). Fail-soft contract identical to above:
// errors bubble; callers wrap in try/catch and render fail-soft banner
// (Phase 02 LEARNINGS P5 dashboard pattern).

export interface L2OhlcRow {
  asset_id: string;
  bucket_ts: string;  // ISO timestamptz
  open: number;
  high: number;
  low: number;
  close: number;
  sample_count: number;
}

export interface L2BookLevel {
  asset_id: string;
  ts: string;
  side: "BUY" | "SELL";
  level: number;
  price: number;
  size: number;
}

/**
 * OHLC bars for a single asset over the last `hours` from the requested
 * granularity view (l2_ohlc_1m / 5m / 1h — Alembic 005).
 * Ascending bucket_ts (lightweight-charts setData expects monotonic time).
 */
export async function getOhlcForAsset(
  assetId: string,
  granularity: "1m" | "5m" | "1h" = "1m",
  hours: number = 24,
  supabase?: SupabaseClient,
): Promise<L2OhlcRow[]> {
  const client = supabase ?? (await getServerSupabase());
  const cutoff = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const view = `l2_ohlc_${granularity}`;
  const { data, error } = await client
    .from(view)
    .select("asset_id, bucket_ts, open, high, low, close, sample_count")
    .eq("asset_id", assetId)
    .gte("bucket_ts", cutoff)
    .order("bucket_ts", { ascending: true });
  if (error) throw error;
  return (data ?? []) as L2OhlcRow[];
}

/**
 * Latest L3 book-levels snapshot (top-20 rows, descending ts) for a single
 * asset. Caller's DepthLadder picks the most recent ts batch and groups by
 * side → top-10 bids + top-10 asks. Returns up to 20 rows to ensure both
 * sides have headroom even when one side reports fewer levels.
 */
export async function getBookLevelsLatest(
  assetId: string,
  supabase?: SupabaseClient,
): Promise<L2BookLevel[]> {
  const client = supabase ?? (await getServerSupabase());
  const { data, error } = await client
    .from("l2_book_levels")
    .select("asset_id, ts, side, level, price, size")
    .eq("asset_id", assetId)
    .order("ts", { ascending: false })
    .limit(20);
  if (error) throw error;
  return (data ?? []) as L2BookLevel[];
}
