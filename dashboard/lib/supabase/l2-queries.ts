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
      "id, snapshot_id, recipe_name, asset_id, market_id, event_id, included_at_ts, removed_at_ts, ranking_score, source",
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
