import assert from "node:assert/strict";

import {
  candidateEnvelopesAgree,
  isCurrentOpportunitiesEnvelope,
  isDiscoveryEnvelope,
  isReconciliationEnvelope,
  isResourcesEnvelope,
  isStatusEnvelope,
} from "../../dashboard/lib/perception.ts";

const coverageWindow = {
  visited_groups: 1,
  raw_fraction: 0.5,
  liquidity_weighted_fraction: 0.5,
};
const validDiscovery = {
  status: "available",
  discovery: {
    next_cursor: null,
    completed: false,
    last_started_at_ms: 1,
    last_finished_at_ms: 2,
    page_event_count: 1,
    groups_seen: 1,
    promoted_count: 1,
    queue_depth_by_class: { high: 1, normal: 0, explore: 0 },
    oldest_visit_age_ms: 1,
    promotion_queue_depth: 1,
    outstanding_admitted_count: 1,
    candidate_attempt_start_count: 1,
    candidate_start_deadline_breach_count: 0,
    candidate_start_ready: true,
    coverage: {
      known_groups: 2,
      total_liquidity_weight: 10,
      by_minutes: {
        15: coverageWindow,
        30: coverageWindow,
        60: coverageWindow,
      },
    },
    load_state: {
      degraded_streak: 1,
      last_reason: "candidate-quote-stale",
      last_decision: "yield",
      probe_every_cycles: 3,
      updated_at_ms: 2,
    },
    admission_proof: {
      effective_capacity: 2,
      candidate_max_wait_ms: 60_000,
      selection_budget_ms: 6_000,
      poll_interval_ms: 1_000,
      group_timeout_ms: 10_000,
      terminal_write_budget_ms: 5_000,
      attempt_start_write_budget_ms: 5_000,
      high_burst_groups: 1,
      reserved_non_high_slots: 3,
      effective_start_bound_ms: 47_000,
    },
  },
};
const validReconciliation = {
  status: "available",
  reconciliation: {
    id: "window",
    status: "applied",
    failure_reason: null,
    next_cursor: null,
    started_at_ms: 1,
    checkpoint_at_ms: 2,
    finished_at_ms: 3,
    pages_completed: 1,
    events_seen: 1,
    groups_staged: 1,
    rejected_count: 0,
    duration_ms: 2,
    observations_count: 1,
    baseline_count: 1,
    added_count: 0,
    changed_count: 0,
    closed_count: 0,
    unchanged_count: 1,
    applied_rejected_count: 0,
  },
};
const validStatus = {
  status: "available",
  server_time_ms: 10,
  candidate_authority_hash: `sha256:${"a".repeat(64)}`,
  current_candidate_group_count: 3,
  candidate_state_counts: {
    watching: 1,
    "no-edge": 1,
    unavailable: 1,
  },
  opportunities: {
    status: "available",
    count: 1,
    reason: "certified-edge",
  },
  open_incident_count: 0,
};
const validCurrentOpportunities = {
  status: "available",
  server_time_ms: 10,
  candidate_authority_hash: `sha256:${"a".repeat(64)}`,
  current_opportunity_count: 1,
  items: [
    {
      group_id: "g-1",
      event_id: "e-1",
      group_revision: 1,
      membership_hash: "membership",
      quote_batch_id: "q-1",
      fact_id: 1,
      bundle_cost: 0.8,
      gross_edge_bps: 2_000,
      max_bundle_size: 10,
      structure_observed_at_ms: 2,
      quote_started_at_ms: 3,
      quote_quoted_at_ms: 4,
    },
  ],
  limit: 1,
  next_after_group_id: null,
};
const validResourceDecision = {
  mode: "normal",
  reason: "candidate-hot-path-fresh",
  reconciliation_enabled: true,
  discovery_batch_limit: 50,
  discovery_duty_multiplier: 1,
  normal_candidate_interval_multiplier: 1,
  high_candidate_interval_multiplier: 1,
  http_preserved: true,
  health_claimed: true,
  previous_discovery_batch_limit: 50,
  decided_at_ms: 2_000,
  policy_version: "opportunity-resource-v1",
  sequence: 1,
  source_sample_id: 1,
  hot_quote_age_ms: 20_000,
  cooldown_ms: 30_000,
  decision_ttl_ms: 15_000,
  valid_until_ms: 17_000,
  mode_changed_at_ms: 2_000,
};
const validResources = {
  status: "available",
  current: validResourceDecision,
  items: [
    {
      sample: {
        candidate_count: 2,
        candidate_quote_p95_ms: 5_000,
        candidate_missing_quote_count: 0,
        candidate_worker_ok: true,
        discovery_worker_ok: true,
        reconciliation_running: true,
        previous_discovery_batch_limit: 50,
        observed_at_ms: 2_000,
      },
      decision: validResourceDecision,
    },
  ],
  limit: 100,
  next_before_sequence: null,
  history_floor: null,
};

const clone = (value) => structuredClone(value);

assert.equal(isDiscoveryEnvelope(validDiscovery), true);
assert.equal(isReconciliationEnvelope(validReconciliation), true);
assert.equal(isStatusEnvelope(validStatus), true);
assert.equal(
  isCurrentOpportunitiesEnvelope(validCurrentOpportunities),
  true,
);
assert.equal(isResourcesEnvelope(validResources), true);
assert.equal(
  candidateEnvelopesAgree(validStatus, validCurrentOpportunities),
  true,
);
{
  const current = clone(validCurrentOpportunities);
  current.candidate_authority_hash = `sha256:${"b".repeat(64)}`;
  assert.equal(
    candidateEnvelopesAgree(validStatus, current),
    false,
    "candidate hashes must bind both envelopes",
  );
}
{
  const current = clone(validCurrentOpportunities);
  current.current_opportunity_count = 2;
  current.next_after_group_id = "g-1";
  assert.equal(
    candidateEnvelopesAgree(validStatus, current),
    false,
    "global opportunity counts must agree",
  );
}
{
  const current = clone(validCurrentOpportunities);
  current.current_opportunity_count = 2;
  assert.equal(
    candidateEnvelopesAgree(
      { ...validStatus, opportunities: { ...validStatus.opportunities, count: 2 } },
      current,
    ),
    false,
    "truncated first page requires a cursor",
  );
}

for (const field of ["watching", "no-edge", "unavailable"]) {
  const negative = clone(validStatus);
  negative.candidate_state_counts[field] = -1;
  assert.equal(isStatusEnvelope(negative), false, `${field}<0 must fail`);
  const fractional = clone(validStatus);
  fractional.candidate_state_counts[field] = 0.5;
  assert.equal(isStatusEnvelope(fractional), false, `${field} must be integer`);
}
{
  const body = clone(validStatus);
  body.current_candidate_group_count = 4;
  assert.equal(isStatusEnvelope(body), false, "state counts must sum to current");
}
{
  const body = clone(validStatus);
  body.opportunities.count = 2;
  assert.equal(isStatusEnvelope(body), false, "edges cannot exceed watching");
}
for (const [field, value] of [
  ["bundle_cost", 0],
  ["gross_edge_bps", -1],
  ["max_bundle_size", Infinity],
  ["fact_id", 0.5],
]) {
  const body = clone(validCurrentOpportunities);
  body.items[0][field] = value;
  assert.equal(
    isCurrentOpportunitiesEnvelope(body),
    false,
    `${field} malformed must fail`,
  );
}
{
  const body = clone(validCurrentOpportunities);
  body.items[0].quote_started_at_ms = 5;
  assert.equal(
    isCurrentOpportunitiesEnvelope(body),
    false,
    "quote timestamps must be ordered",
  );
}
for (const mutate of [
  (body) => {
    body.current.valid_until_ms += 1;
  },
  (body) => {
    body.items[0].sample.candidate_missing_quote_count = 3;
  },
  (body) => {
    body.items[0].decision = {
      ...body.items[0].decision,
      sequence: 2,
    };
  },
  (body) => {
    body.next_before_sequence = 1;
  },
  (body) => {
    body.current.reason = "forged-reason";
  },
  (body) => {
    body.current.policy_version = "opportunity-resource-v2";
  },
]) {
  const body = clone(validResources);
  mutate(body);
  assert.equal(
    isResourcesEnvelope(body),
    false,
    "malformed resource authority must fail",
  );
}
{
  const body = clone(validCurrentOpportunities);
  body.next_after_group_id = "wrong";
  assert.equal(
    isCurrentOpportunitiesEnvelope(body),
    false,
    "cursor must bind to final row",
  );
}

for (const field of [
  "candidate_max_wait_ms",
  "selection_budget_ms",
  "poll_interval_ms",
  "group_timeout_ms",
  "high_burst_groups",
  "reserved_non_high_slots",
]) {
  const body = clone(validDiscovery);
  body.discovery.admission_proof[field] = 0;
  assert.equal(isDiscoveryEnvelope(body), false, `${field}=0 must fail`);
}

{
  const body = clone(validDiscovery);
  body.discovery.admission_proof.reserved_non_high_slots = 1;
  assert.equal(isDiscoveryEnvelope(body), false, "capacity must fit reserved slots");
}
{
  const body = clone(validDiscovery);
  body.discovery.admission_proof.effective_start_bound_ms = 60_001;
  assert.equal(isDiscoveryEnvelope(body), false, "start bound must fit max wait");
}
{
  const body = clone(validDiscovery);
  body.discovery.candidate_start_deadline_breach_count = 2;
  assert.equal(isDiscoveryEnvelope(body), false, "breaches cannot exceed attempts");
}
{
  const body = clone(validDiscovery);
  body.discovery.candidate_start_deadline_breach_count = 1;
  assert.equal(
    isDiscoveryEnvelope(body),
    false,
    "candidate readiness must reflect deadline breaches",
  );
}
{
  const body = clone(validDiscovery);
  body.discovery.outstanding_admitted_count = 3;
  assert.equal(
    isDiscoveryEnvelope(body),
    false,
    "outstanding admissions cannot exceed capacity",
  );
}
{
  const body = clone(validDiscovery);
  body.discovery.coverage.by_minutes[15].visited_groups = 3;
  assert.equal(isDiscoveryEnvelope(body), false, "visits cannot exceed known groups");
}
{
  const body = clone(validDiscovery);
  body.discovery.coverage.known_groups = 0.5;
  assert.equal(isDiscoveryEnvelope(body), false, "counts must be integers");
}

for (const field of [
  "added_count",
  "changed_count",
  "closed_count",
  "unchanged_count",
  "applied_rejected_count",
]) {
  const negative = clone(validReconciliation);
  negative.reconciliation[field] = -1;
  assert.equal(isReconciliationEnvelope(negative), false, `${field}<0 must fail`);
  const fractional = clone(validReconciliation);
  fractional.reconciliation[field] = 0.5;
  assert.equal(
    isReconciliationEnvelope(fractional),
    false,
    `${field} must be integer`,
  );
}

{
  const partial = clone(validReconciliation);
  partial.reconciliation.added_count = null;
  assert.equal(isReconciliationEnvelope(partial), false, "diffs are all-or-none");
}
{
  const unapplied = clone(validReconciliation);
  unapplied.reconciliation.status = "complete";
  assert.equal(
    isReconciliationEnvelope(unapplied),
    false,
    "only applied windows expose diff counts",
  );
}
{
  const appliedWithoutDiff = clone(validReconciliation);
  for (const field of [
    "added_count",
    "changed_count",
    "closed_count",
    "unchanged_count",
    "applied_rejected_count",
  ]) {
    appliedWithoutDiff.reconciliation[field] = null;
  }
  assert.equal(
    isReconciliationEnvelope(appliedWithoutDiff),
    false,
    "applied windows require every diff count",
  );
}
