import assert from "node:assert/strict";

import {
  isDiscoveryEnvelope,
  isReconciliationEnvelope,
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

const clone = (value) => structuredClone(value);

assert.equal(isDiscoveryEnvelope(validDiscovery), true);
assert.equal(isReconciliationEnvelope(validReconciliation), true);

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
