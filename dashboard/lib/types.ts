// Phase 02 Plan 02-06 — TypeScript mirror of Supabase narrow schema (Alembic 001 + 002).
// Plan 03 snapshots/markets_latest, Plan 02-08 top_movers_view.

export type SnapshotStatus =
  "OK" | "DEGRADED" | "FAILED" | "pass" | "warn" | "fail";

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
  "discovered" | "certified" | "stale" | "invalidated" | "closed";

export interface PerceptionUnavailable {
  status: "unavailable";
  reason: string;
}

export interface PerceptionAvailable<T> {
  status: "available";
  data: T;
}

export type PerceptionReadResult<T> =
  PerceptionAvailable<T> | PerceptionUnavailable;

export interface PerceptionOpportunityStatus {
  status: PerceptionAvailability;
  count: number | null;
  reason: string;
}

export interface PerceptionStatusEnvelope {
  status: "available";
  server_time_ms: number;
  candidate_authority_hash: string;
  current_candidate_group_count: number;
  candidate_state_counts: {
    watching: number;
    "no-edge": number;
    unavailable: number;
  };
  opportunities: PerceptionOpportunityStatus;
  open_incident_count: number;
}

export interface PerceptionHealthCheck {
  componentId: string;
  componentType?: string;
  observedValue: unknown;
  observedUnit?: string;
  status: "pass" | "warn" | "fail";
  output?: string;
  impact?: string;
  automaticAction?: string;
  operatorAction?: string;
}

export interface PerceptionHealthEnvelope {
  status: "pass" | "warn" | "fail";
  releaseId: string;
  machineId: string;
  bootId: string;
  checks: Record<string, PerceptionHealthCheck[]>;
}

export interface PerceptionCurrentOpportunity {
  group_id: string;
  event_id: string;
  group_revision: number;
  membership_hash: string;
  quote_batch_id: string;
  fact_id: number;
  bundle_cost: number;
  gross_edge_bps: number;
  max_bundle_size: number;
  structure_observed_at_ms: number;
  quote_started_at_ms: number;
  quote_quoted_at_ms: number;
}

export interface PerceptionCurrentOpportunitiesEnvelope {
  status: "available";
  server_time_ms: number;
  candidate_authority_hash: string;
  current_opportunity_count: number;
  items: PerceptionCurrentOpportunity[];
  limit: number;
  next_after_group_id: string | null;
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

interface PerceptionTimelineBase {
  stable_id: number;
  occurred_at_ms: number;
}

export interface PerceptionMembershipTimelineItem extends PerceptionTimelineBase {
  class: "membership_revision";
  group_id: string;
  event_id: string;
  revision: number;
  membership_hash: string;
  status: PerceptionGroupStatus;
  leg_count: number;
  source_cursor: string;
}

export interface PerceptionQuoteTimelineItem extends PerceptionTimelineBase {
  class: "quote_batch";
  quote_batch_id: string;
  group_revision: number;
  membership_hash: string;
  status: "complete" | "failed" | "superseded";
  failure_reason: string | null;
  leg_count: number;
  duration_ms: number;
}

export interface PerceptionOpportunityTimelineState {
  last_result: "watching" | "no-edge" | "unavailable";
  opportunity: boolean;
}

export interface PerceptionOpportunityTimelineItem extends PerceptionTimelineBase {
  class: "opportunity_transition";
  from: PerceptionOpportunityTimelineState | null;
  to: PerceptionOpportunityTimelineState;
  reason: string | null;
  quote_batch_id: string | null;
  gross_edge_bps: number | null;
}

export interface PerceptionIncidentTimelineItem extends PerceptionTimelineBase {
  class: "incident_event";
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
  evidence: Record<string, unknown>;
}

export type PerceptionGroupTimelineItem =
  | PerceptionMembershipTimelineItem
  | PerceptionQuoteTimelineItem
  | PerceptionOpportunityTimelineItem
  | PerceptionIncidentTimelineItem;

export interface PerceptionGroupTimelineEnvelope {
  status: "available";
  group_id: string;
  items: PerceptionGroupTimelineItem[];
  limit: number;
  next_before: string | null;
  history_floor: {
    membership: {
      scope: "global";
      through_id: number;
      compacted_count: number;
    };
    quote: {
      scope: "global";
      through_id: number;
      compacted_count: number;
    };
    opportunity: {
      scope: "global";
      through_id: number;
      source_rows_compacted: number;
    };
    incident: {
      scope: string;
      through_id: number;
      compacted_count: number;
    };
  };
  history_complete: {
    membership: boolean;
    quote: boolean;
    opportunity: boolean;
    incident: boolean;
  };
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
  detected_at_ms: number;
  lifecycle_age_ms: number;
  action:
    | "classify-producer-failure"
    | "operator-intervention"
    | "restart-producer"
    | "retry-producer"
    | null;
  retry_count: number | null;
  next_retry_at_ms: number | null;
  recovery_occurred_at_ms: number | null;
  recovery_start_evidence: Record<string, unknown> | null;
  history_floor: {
    through_event_id: number;
    compacted_event_count: number;
  } | null;
  notification_delivery_tracked: false;
  diagnosis: {
    severity: "p1" | "p2";
    reminder_interval_s: number;
    impact:
      | "feed-at-risk"
      | "feed-unavailable"
      | "storage-exhaustion-risk"
      | "market-map-stale";
    automatic_action:
      | "retry-immediately"
      | "retry-at-next-cadence"
      | "reclaim-bounded-history"
      | "retry-supervised-producer"
      | "automatic-retries-exhausted"
      | "retry-bounded-structure-child";
    next_action:
      | "inspect-clob-and-child-io"
      | "inspect-child-stderr"
      | "inspect-capacity-receipts"
      | "inspect-producer-receipt-and-restart"
      | "inspect-stage-checkpoint-and-child-budget";
    deadline_s: number | null;
    consecutive_failures: number;
    last_success_age_s: number | null;
    free_percent: number | null;
    failure_reason: string | null;
    elapsed_ms?: number | null;
    last_stage?: string | null;
    cooperative_slice_budget_s?: number;
    child_hard_limit_s?: number;
  } | null;
  evidence: Record<string, unknown>;
}

export interface PerceptionIncidentsEnvelope {
  status: "available";
  items: PerceptionIncident[];
  limit: number;
  open_count: number;
  next_before: string | null;
}

export interface PerceptionResourceSample {
  candidate_count: number;
  candidate_quote_p95_ms: number | null;
  candidate_missing_quote_count: number;
  candidate_worker_ok: boolean;
  discovery_worker_ok: boolean;
  reconciliation_running: boolean;
  previous_discovery_batch_limit: number;
  observed_at_ms: number;
}

export interface PerceptionResourceDecision {
  mode: "normal" | "protect-hot-path" | "empty-candidate-exploration";
  reason: string;
  reconciliation_enabled: boolean;
  discovery_batch_limit: number;
  discovery_duty_multiplier: number;
  normal_candidate_interval_multiplier: number;
  high_candidate_interval_multiplier: number;
  http_preserved: boolean;
  health_claimed: boolean;
  previous_discovery_batch_limit: number;
  decided_at_ms: number;
  policy_version: string;
  sequence: number;
  source_sample_id: number;
  hot_quote_age_ms: number;
  cooldown_ms: number;
  decision_ttl_ms: number;
  valid_until_ms: number;
  mode_changed_at_ms: number;
}

export interface PerceptionResourceHistoryItem {
  sample: PerceptionResourceSample;
  decision: PerceptionResourceDecision;
}

export interface PerceptionResourcesEnvelope {
  status: "available";
  current: PerceptionResourceDecision | null;
  items: PerceptionResourceHistoryItem[];
  limit: number;
  next_before_sequence: number | null;
  history_floor: {
    through_sample_id: number;
    through_decision_id: number;
    through_sequence: number;
    compacted_sample_count: number;
    compacted_decision_count: number;
  } | null;
}

export interface PerceptionProducerAttempt {
  id: number;
  checkpoint_at_ms?: number;
  phase?: string;
  outcome?: string;
  failure_kind?: string | null;
  target_count?: number | null;
  phase_timings?: Record<string, number>;
  last_stage?: string | null;
  elapsed_ms?: number | null;
  chunks_processed?: number | null;
  stderr_tail?: string | null;
}

export interface PerceptionProducerProgressEnvelope {
  status: "available";
  quote: { attempt: PerceptionProducerAttempt | null };
  structure: {
    attempt: PerceptionProducerAttempt | null;
    comparison?: {
      publication_id: string;
      generation_snapshot_id: number;
      phase: string;
      phase_row_count: number;
      checkpoint_at_ms: number;
    } | null;
  };
  automatic_action: string;
  operator_action: string;
}

export interface PerceptionOverview {
  health: PerceptionHealthEnvelope;
  status: PerceptionStatusEnvelope;
  currentOpportunities: PerceptionCurrentOpportunitiesEnvelope;
  groups: PerceptionGroupsEnvelope;
  discovery: PerceptionDiscoveryEnvelope;
  reconciliation: PerceptionReconciliationEnvelope;
  incidents: PerceptionIncidentsEnvelope;
  resources: PerceptionResourcesEnvelope;
  producerProgress: PerceptionProducerProgressEnvelope | null;
}

export interface PerceptionGroupDetail {
  timeline: PerceptionGroupTimelineEnvelope;
}
