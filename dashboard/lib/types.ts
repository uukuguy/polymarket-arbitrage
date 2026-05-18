// Phase 02 Plan 02-06 — TypeScript mirror of Supabase narrow schema (Alembic 001 + 002).
// Plan 03 snapshots/markets_latest, Plan 02-08 top_movers_view.

export type SnapshotStatus = "OK" | "DEGRADED" | "FAILED" | "pass" | "warn" | "fail";

// Snapshot row — Alembic 001 schema exactly (8 columns).
// Earlier draft included parquet_r2_url / supabase_mirror_at_ms / is_valid which
// don't exist in the actual table — see /status page comment for context.
export interface Snapshot {
  id: number;
  taken_at_ms: number;
  finished_at_ms: number | null;
  mode: "subset" | "full";
  status: SnapshotStatus;
  market_count: number;
  parquet_url: string | null;
  issue_count_by_layer: Record<string, number> | null;
}

export interface MarketLatest {
  market_id: string;
  question: string;
  slug: string;
  event_slug: string;
  mid_price: number;
  liquidity_usd: number;
  volume_usd: number;
  end_time_ms: number | null;
  snapshot_id: number;
  question_zh: string | null;
}

// top_movers_view (Alembic 002) — informational-interest proxy (proximity to 0.5).
// Plan 02-06 plan called for cross-snapshot price-delta, but Alembic 002
// shipped against the existing markets_latest full-overwrite schema.
// Real time-windowed delta is deferred to Phase 02.1 (needs markets_history table).
export interface TopMoverRow {
  market_id: string;
  question: string;
  question_zh: string | null;
  slug: string | null;
  event_slug: string | null;
  mid_price: number;
  liquidity_usd: number | null;
  volume_usd: number | null;
  end_time_ms: number | null;
  snapshot_id: number;
  uncertainty_score: number;
}

// /api/scan request/response (mirrors Fly daemon src/polyarb/http/scan.py).
export interface ScanRequestBody {
  recipe_name: string;
  params?: Record<string, unknown>;
}

export interface ScanResponse {
  recipe?: string;
  row_count?: number;
  rows?: unknown[];
  error?: string;
}
