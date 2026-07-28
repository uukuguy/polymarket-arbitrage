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

// Task 6 bounded public perception read models.
export type PerceptionAvailability = "available" | "unavailable";
export type PerceptionGroupStatus =
  | "discovered"
  | "certified"
  | "stale"
  | "invalidated"
  | "closed";

export interface PerceptionUnavailable {
  status: "unavailable";
  reason: string;
}

export interface PerceptionAvailable<T> {
  status: "available";
  data: T;
}

export type PerceptionReadResult<T> =
  | PerceptionAvailable<T>
  | PerceptionUnavailable;

export interface PerceptionOpportunityStatus {
  status: PerceptionAvailability;
  count: number | null;
  reason: string;
}

export interface PerceptionStatusEnvelope {
  status: "available";
  opportunities: PerceptionOpportunityStatus;
  open_incident_count: number;
}

export interface PerceptionGroupRevision {
  group_id: string;
  event_id: string;
  revision: number;
  membership_hash: string;
  status: PerceptionGroupStatus;
  started_at_ms: number;
  observed_at_ms: number;
  source_cursor: string;
  leg_count: number;
}

export interface PerceptionGroupsEnvelope {
  status: "available";
  items: PerceptionGroupRevision[];
  limit: number;
  next_after: string | null;
}

export interface PerceptionGroupHistoryEnvelope {
  status: "available";
  group_id: string;
  items: PerceptionGroupRevision[];
  limit: number;
  next_before_revision: number | null;
}

export interface PerceptionDiscoveryStatus {
  next_cursor: string | null;
  completed: boolean;
  last_started_at_ms: number | null;
  last_finished_at_ms: number | null;
  page_event_count: number;
  groups_seen: number;
  promoted_count: number;
  queue_depth_by_class: Record<string, number>;
  oldest_visit_age_ms: number | null;
  promotion_queue_depth: number;
  outstanding_admitted_count: number;
  candidate_attempt_start_count: number;
  candidate_start_deadline_breach_count: number;
  candidate_start_ready: boolean;
  coverage: {
    known_groups: number;
    total_liquidity_weight: number;
    by_minutes: Record<
      "15" | "30" | "60",
      {
        visited_groups: number;
        raw_fraction: number;
        liquidity_weighted_fraction: number;
      }
    >;
  };
  load_state: {
    degraded_streak: number;
    last_reason: string | null;
    last_decision: "fresh" | "yield" | "probe";
    probe_every_cycles: number;
    updated_at_ms: number;
  };
  admission_proof: {
    effective_capacity: number;
    candidate_max_wait_ms: number;
    selection_budget_ms: number;
    poll_interval_ms: number;
    group_timeout_ms: number;
    terminal_write_budget_ms: number;
    attempt_start_write_budget_ms: number;
    high_burst_groups: number;
    reserved_non_high_slots: number;
    effective_start_bound_ms: number | null;
  } | null;
}

export interface PerceptionDiscoveryEnvelope {
  status: "available";
  discovery: PerceptionDiscoveryStatus | null;
}

export interface PerceptionReconciliationStatus {
  id: string;
  status: "open" | "complete" | "applied" | "failed";
  failure_reason: string | null;
  next_cursor: string | null;
  started_at_ms: number;
  checkpoint_at_ms: number;
  finished_at_ms: number | null;
  pages_completed: number;
  events_seen: number;
  groups_staged: number;
  rejected_count: number;
  duration_ms: number;
  observations_count: number;
  baseline_count: number;
  added_count: number | null;
  changed_count: number | null;
  closed_count: number | null;
  unchanged_count: number | null;
  applied_rejected_count: number | null;
}

export interface PerceptionReconciliationEnvelope {
  status: "available";
  reconciliation: PerceptionReconciliationStatus | null;
}

export interface PerceptionIncident {
  incident_id: string;
  sequence: number;
  scope: string;
  kind: string;
  state:
    | "detected"
    | "classified"
    | "contained"
    | "recovering"
    | "verified"
    | "escalated";
  occurred_at_ms: number;
  evidence: Record<string, unknown>;
}

export interface PerceptionIncidentsEnvelope {
  status: "available";
  items: PerceptionIncident[];
  limit: number;
}

export interface PerceptionOverview {
  status: PerceptionStatusEnvelope;
  groups: PerceptionGroupsEnvelope;
  discovery: PerceptionDiscoveryEnvelope;
  reconciliation: PerceptionReconciliationEnvelope;
  incidents: PerceptionIncidentsEnvelope;
}

export interface PerceptionGroupDetail {
  history: PerceptionGroupHistoryEnvelope;
  incidents: PerceptionIncidentsEnvelope;
}
