import type {
  PerceptionCurrentOpportunitiesEnvelope,
  PerceptionDiscoveryEnvelope,
  PerceptionGroupDetail,
  PerceptionGroupHistoryEnvelope,
  PerceptionGroupTimelineEnvelope,
  PerceptionGroupsEnvelope,
  PerceptionHealthEnvelope,
  PerceptionIncidentsEnvelope,
  PerceptionOverview,
  PerceptionReadResult,
  PerceptionReconciliationEnvelope,
  PerceptionResourceDecision,
  PerceptionResourceSample,
  PerceptionResourcesEnvelope,
  PerceptionStatusEnvelope,
} from "@/lib/types";

const PERCEPTION_BASE_URL =
  process.env.POLYARB_L1_URL ?? "https://polyarb-l1.fly.dev";
const GROUP_LIMIT = 100;
const OPPORTUNITY_LIMIT = 100;
const INCIDENT_LIMIT = 500;
const RESOURCE_LIMIT = 100;
const HISTORY_LIMIT = 100;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isStringOrNull(value: unknown): value is string | null {
  return typeof value === "string" || value === null;
}

function isSha256(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^sha256:[0-9a-f]{64}$/.test(value)
  );
}

function isNumberOrNull(value: unknown): value is number | null {
  return (typeof value === "number" && Number.isFinite(value)) || value === null;
}

function isNonNegativeNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function isNonNegativeInteger(value: unknown): value is number {
  return isNonNegativeNumber(value) && Number.isInteger(value);
}

function isPositiveInteger(value: unknown): value is number {
  return isNonNegativeInteger(value) && value > 0;
}

function isNonNegativeIntegerOrNull(value: unknown): value is number | null {
  return value === null || isNonNegativeInteger(value);
}

function isFraction(value: unknown): value is number {
  return isNonNegativeNumber(value) && value <= 1;
}

function isCoverageWindow(value: unknown): value is {
  visited_groups: number;
  raw_fraction: number;
  liquidity_weighted_fraction: number;
} {
  return (
    isRecord(value) &&
    isNonNegativeInteger(value.visited_groups) &&
    isFraction(value.raw_fraction) &&
    isFraction(value.liquidity_weighted_fraction)
  );
}

type AdmissionProofContract = {
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
};

function isAdmissionProof(
  value: unknown,
): value is AdmissionProofContract | null {
  if (value === null) return true;
  if (
    !isRecord(value) ||
    !isNonNegativeInteger(value.effective_capacity) ||
    !isPositiveInteger(value.candidate_max_wait_ms) ||
    value.candidate_max_wait_ms > 60_000 ||
    !isPositiveInteger(value.selection_budget_ms) ||
    !isPositiveInteger(value.poll_interval_ms) ||
    !isPositiveInteger(value.group_timeout_ms) ||
    !isNonNegativeInteger(value.terminal_write_budget_ms) ||
    value.terminal_write_budget_ms < 5_000 ||
    !isNonNegativeInteger(value.attempt_start_write_budget_ms) ||
    value.attempt_start_write_budget_ms < 5_000 ||
    !isPositiveInteger(value.high_burst_groups) ||
    !isPositiveInteger(value.reserved_non_high_slots) ||
    value.effective_capacity > value.reserved_non_high_slots
  ) {
    return false;
  }
  const expectedBound =
    value.effective_capacity === 0
      ? null
      : value.poll_interval_ms +
        value.selection_budget_ms +
        value.effective_capacity * value.attempt_start_write_budget_ms +
        (value.high_burst_groups + value.effective_capacity - 1) *
          (value.group_timeout_ms + value.terminal_write_budget_ms);
  return (
    value.effective_start_bound_ms === expectedBound &&
    (expectedBound === null || expectedBound <= value.candidate_max_wait_ms)
  );
}

function isGroupRevision(value: unknown): boolean {
  if (!isRecord(value)) return false;
  return (
    typeof value.group_id === "string" &&
    typeof value.event_id === "string" &&
    typeof value.revision === "number" &&
    typeof value.membership_hash === "string" &&
    ["discovered", "certified", "stale", "invalidated", "closed"].includes(
      String(value.status),
    ) &&
    typeof value.started_at_ms === "number" &&
    typeof value.observed_at_ms === "number" &&
    typeof value.source_cursor === "string" &&
    typeof value.leg_count === "number"
  );
}

export function isStatusEnvelope(
  value: unknown,
): value is PerceptionStatusEnvelope {
  if (!isRecord(value) || value.status !== "available") return false;
  const opportunities = value.opportunities;
  const stateCounts = value.candidate_state_counts;
  if (
    !isRecord(stateCounts) ||
    !isNonNegativeInteger(stateCounts.watching) ||
    !isNonNegativeInteger(stateCounts["no-edge"]) ||
    !isNonNegativeInteger(stateCounts.unavailable) ||
    !isNonNegativeInteger(value.current_candidate_group_count) ||
    stateCounts.watching +
      stateCounts["no-edge"] +
      stateCounts.unavailable !==
      value.current_candidate_group_count
  ) {
    return false;
  }
  return (
    isRecord(opportunities) &&
    ["available", "unavailable"].includes(String(opportunities.status)) &&
    isNonNegativeIntegerOrNull(opportunities.count) &&
    typeof opportunities.reason === "string" &&
    (opportunities.count === null ||
      opportunities.count <= stateCounts.watching) &&
    isNonNegativeInteger(value.server_time_ms) &&
    isSha256(value.candidate_authority_hash) &&
    isNonNegativeInteger(value.open_incident_count)
  );
}

export function isCurrentOpportunitiesEnvelope(
  value: unknown,
): value is PerceptionCurrentOpportunitiesEnvelope {
  if (
    !isRecord(value) ||
    value.status !== "available" ||
    !isNonNegativeInteger(value.server_time_ms) ||
    !isSha256(value.candidate_authority_hash) ||
    !isNonNegativeInteger(value.current_opportunity_count) ||
    !Array.isArray(value.items) ||
    !isPositiveInteger(value.limit) ||
    value.limit > 500 ||
    value.items.length > value.limit ||
    value.items.length > value.current_opportunity_count ||
    !isStringOrNull(value.next_after_group_id)
  ) {
    return false;
  }
  let previousGroupId = "";
  for (const item of value.items) {
    if (
      !isRecord(item) ||
      typeof item.group_id !== "string" ||
      !item.group_id ||
      item.group_id <= previousGroupId ||
      typeof item.event_id !== "string" ||
      !item.event_id ||
      !isPositiveInteger(item.group_revision) ||
      typeof item.membership_hash !== "string" ||
      !item.membership_hash ||
      typeof item.quote_batch_id !== "string" ||
      !item.quote_batch_id ||
      !isPositiveInteger(item.fact_id) ||
      !isNonNegativeNumber(item.bundle_cost) ||
      item.bundle_cost === 0 ||
      !isNonNegativeNumber(item.gross_edge_bps) ||
      item.gross_edge_bps === 0 ||
      !isNonNegativeNumber(item.max_bundle_size) ||
      item.max_bundle_size === 0 ||
      !isNonNegativeInteger(item.structure_observed_at_ms) ||
      !isNonNegativeInteger(item.quote_started_at_ms) ||
      !isNonNegativeInteger(item.quote_quoted_at_ms) ||
      item.quote_started_at_ms > item.quote_quoted_at_ms
    ) {
      return false;
    }
    previousGroupId = item.group_id;
  }
  return (
    value.next_after_group_id === null ||
    (value.items.length === value.limit &&
      value.items.length > 0 &&
      value.next_after_group_id === previousGroupId)
  );
}

function isGroupsEnvelope(value: unknown): value is PerceptionGroupsEnvelope {
  return (
    isRecord(value) &&
    value.status === "available" &&
    Array.isArray(value.items) &&
    value.items.every(isGroupRevision) &&
    typeof value.limit === "number" &&
    isStringOrNull(value.next_after)
  );
}

function isGroupHistoryEnvelope(
  value: unknown,
  expectedGroupId: string,
): value is PerceptionGroupHistoryEnvelope {
  return (
    isRecord(value) &&
    value.status === "available" &&
    value.group_id === expectedGroupId &&
    Array.isArray(value.items) &&
    value.items.every(
      (item) =>
        isGroupRevision(item) &&
        isRecord(item) &&
        item.group_id === expectedGroupId,
    ) &&
    typeof value.limit === "number" &&
    isNumberOrNull(value.next_before_revision)
  );
}

const TIMELINE_CLASS_ORDER = {
  membership_revision: 0,
  quote_batch: 1,
  opportunity_transition: 2,
  incident_event: 3,
} as const;

function isTimelineState(
  value: unknown,
): value is { last_result: string; opportunity: boolean } {
  return (
    isRecord(value) &&
    ["watching", "no-edge", "unavailable"].includes(
      String(value.last_result),
    ) &&
    typeof value.opportunity === "boolean" &&
    (value.opportunity === false || value.last_result === "watching")
  );
}

function isTimelineItem(value: unknown, expectedGroupId: string): boolean {
  if (
    !isRecord(value) ||
    !isPositiveInteger(value.stable_id) ||
    !isNonNegativeInteger(value.occurred_at_ms) ||
    !Object.hasOwn(TIMELINE_CLASS_ORDER, String(value.class))
  ) {
    return false;
  }
  if (value.class === "membership_revision") {
    return (
      value.group_id === expectedGroupId &&
      typeof value.event_id === "string" &&
      value.event_id.length > 0 &&
      isPositiveInteger(value.revision) &&
      typeof value.membership_hash === "string" &&
      value.membership_hash.length > 0 &&
      ["discovered", "certified", "stale", "invalidated", "closed"].includes(
        String(value.status),
      ) &&
      isNonNegativeInteger(value.leg_count) &&
      typeof value.source_cursor === "string"
    );
  }
  if (value.class === "quote_batch") {
    return (
      typeof value.quote_batch_id === "string" &&
      value.quote_batch_id.length > 0 &&
      isPositiveInteger(value.group_revision) &&
      typeof value.membership_hash === "string" &&
      value.membership_hash.length > 0 &&
      ["complete", "failed", "superseded"].includes(String(value.status)) &&
      isStringOrNull(value.failure_reason) &&
      isNonNegativeInteger(value.leg_count) &&
      isNonNegativeInteger(value.duration_ms) &&
      (value.status === "failed"
        ? typeof value.failure_reason === "string" &&
          value.failure_reason.length > 0 &&
          value.leg_count === 0
        : value.failure_reason === null)
    );
  }
  if (value.class === "opportunity_transition") {
    const fromValid = value.from === null || isTimelineState(value.from);
    const toValid = isTimelineState(value.to);
    return (
      fromValid &&
      toValid &&
      isStringOrNull(value.reason) &&
      isStringOrNull(value.quote_batch_id) &&
      (value.gross_edge_bps === null ||
        typeof value.gross_edge_bps === "number") &&
      (value.from === null ||
        (isRecord(value.from) &&
          isRecord(value.to) &&
          (value.from.last_result !== value.to.last_result ||
            value.from.opportunity !== value.to.opportunity)))
    );
  }
  return (
    typeof value.incident_id === "string" &&
    value.incident_id.length > 0 &&
    isPositiveInteger(value.sequence) &&
    value.scope === `candidate:${expectedGroupId}` &&
    typeof value.kind === "string" &&
    value.kind.length > 0 &&
    [
      "detected",
      "classified",
      "contained",
      "recovering",
      "verified",
      "escalated",
    ].includes(String(value.state)) &&
    isRecord(value.evidence)
  );
}

function isGroupTimelineEnvelope(
  value: unknown,
  expectedGroupId: string,
): value is PerceptionGroupTimelineEnvelope {
  if (
    !isRecord(value) ||
    value.status !== "available" ||
    value.group_id !== expectedGroupId ||
    !Array.isArray(value.items) ||
    !isPositiveInteger(value.limit) ||
    value.limit > 500 ||
    value.items.length > value.limit ||
    !isStringOrNull(value.next_before) ||
    !isRecord(value.history_floor) ||
    !isRecord(value.history_complete) ||
    Object.keys(value.history_floor).sort().join(",") !==
      "incident,membership,opportunity,quote" ||
    Object.keys(value.history_complete).sort().join(",") !==
      "incident,membership,opportunity,quote"
  ) {
    return false;
  }
  const floor = value.history_floor;
  const complete = value.history_complete;
  const membershipFloor = floor.membership;
  const quoteFloor = floor.quote;
  const incidentFloor = floor.incident;
  for (const item of [membershipFloor, quoteFloor, incidentFloor]) {
    if (
      !isRecord(item) ||
      !isNonNegativeInteger(item.through_id) ||
      !isNonNegativeInteger(item.compacted_count)
    ) {
      return false;
    }
  }
  if (
    !isRecord(membershipFloor) ||
    !isRecord(quoteFloor) ||
    !isRecord(incidentFloor) ||
    membershipFloor.scope !== "global" ||
    quoteFloor.scope !== "global" ||
    !isRecord(floor.opportunity) ||
    floor.opportunity.scope !== "global" ||
    !isNonNegativeInteger(floor.opportunity.through_id) ||
    !isNonNegativeInteger(floor.opportunity.source_rows_compacted) ||
    incidentFloor.scope !== `candidate:${expectedGroupId}` ||
    complete.membership !== (membershipFloor.compacted_count === 0) ||
    complete.quote !== (quoteFloor.compacted_count === 0) ||
    complete.opportunity !==
      (floor.opportunity.source_rows_compacted === 0) ||
    complete.incident !== (incidentFloor.compacted_count === 0)
  ) {
    return false;
  }
  let previous: Record<string, unknown> | null = null;
  for (const item of value.items) {
    if (!isTimelineItem(item, expectedGroupId) || !isRecord(item)) return false;
    if (previous !== null) {
      const priorClass =
        TIMELINE_CLASS_ORDER[
          previous.class as keyof typeof TIMELINE_CLASS_ORDER
        ];
      const itemClass =
        TIMELINE_CLASS_ORDER[item.class as keyof typeof TIMELINE_CLASS_ORDER];
      if (
        Number(item.occurred_at_ms) > Number(previous.occurred_at_ms) ||
        (item.occurred_at_ms === previous.occurred_at_ms &&
          (itemClass < priorClass ||
            (itemClass === priorClass &&
              Number(item.stable_id) >= Number(previous.stable_id))))
      ) {
        return false;
      }
    }
    previous = item;
  }
  return (
    value.next_before === null ||
    (value.items.length === value.limit && value.items.length > 0)
  );
}

export function isDiscoveryEnvelope(
  value: unknown,
): value is PerceptionDiscoveryEnvelope {
  if (!isRecord(value) || value.status !== "available") return false;
  if (value.discovery === null) return true;
  const discovery = value.discovery;
  const coverage = isRecord(discovery) ? discovery.coverage : null;
  const windows = isRecord(coverage) ? coverage.by_minutes : null;
  const loadState = isRecord(discovery) ? discovery.load_state : null;
  const queue = isRecord(discovery) ? discovery.queue_depth_by_class : null;
  const admissionProof = isRecord(discovery)
    ? discovery.admission_proof
    : undefined;
  return (
    isRecord(discovery) &&
    isStringOrNull(discovery.next_cursor) &&
    typeof discovery.completed === "boolean" &&
    isNumberOrNull(discovery.last_started_at_ms) &&
    isNumberOrNull(discovery.last_finished_at_ms) &&
    isNonNegativeInteger(discovery.page_event_count) &&
    isNonNegativeInteger(discovery.groups_seen) &&
    discovery.groups_seen <= discovery.page_event_count &&
    isNonNegativeInteger(discovery.promoted_count) &&
    discovery.promoted_count <= discovery.groups_seen &&
    isRecord(queue) &&
    Object.keys(queue).sort().join(",") === "explore,high,normal" &&
    Object.values(queue).every(isNonNegativeInteger) &&
    isNonNegativeIntegerOrNull(discovery.oldest_visit_age_ms) &&
    isNonNegativeInteger(discovery.promotion_queue_depth) &&
    isNonNegativeInteger(discovery.outstanding_admitted_count) &&
    isNonNegativeInteger(discovery.candidate_attempt_start_count) &&
    isNonNegativeInteger(discovery.candidate_start_deadline_breach_count) &&
    discovery.candidate_start_deadline_breach_count <=
      discovery.candidate_attempt_start_count &&
    discovery.candidate_start_ready ===
      (discovery.candidate_start_deadline_breach_count === 0) &&
    isRecord(coverage) &&
    isNonNegativeInteger(coverage.known_groups) &&
    isNonNegativeNumber(coverage.total_liquidity_weight) &&
    isRecord(windows) &&
    isCoverageWindow(windows["15"]) &&
    windows["15"].visited_groups <= coverage.known_groups &&
    isCoverageWindow(windows["30"]) &&
    windows["30"].visited_groups <= coverage.known_groups &&
    isCoverageWindow(windows["60"]) &&
    windows["60"].visited_groups <= coverage.known_groups &&
    isRecord(loadState) &&
    isNonNegativeInteger(loadState.degraded_streak) &&
    isStringOrNull(loadState.last_reason) &&
    ["fresh", "yield", "probe"].includes(String(loadState.last_decision)) &&
    isPositiveInteger(loadState.probe_every_cycles) &&
    loadState.probe_every_cycles >= 2 &&
    isNonNegativeInteger(loadState.updated_at_ms) &&
    isAdmissionProof(admissionProof) &&
    (admissionProof === null
      ? discovery.outstanding_admitted_count === 0
      : discovery.outstanding_admitted_count <=
        admissionProof.effective_capacity)
  );
}

export function isReconciliationEnvelope(
  value: unknown,
): value is PerceptionReconciliationEnvelope {
  if (!isRecord(value) || value.status !== "available") return false;
  if (value.reconciliation === null) return true;
  const reconciliation = value.reconciliation;
  const diffCounts = isRecord(reconciliation)
    ? [
        reconciliation.added_count,
        reconciliation.changed_count,
        reconciliation.closed_count,
        reconciliation.unchanged_count,
        reconciliation.applied_rejected_count,
      ]
    : [];
  const allDiffsPresent =
    diffCounts.length === 5 && diffCounts.every((item) => item !== null);
  const allDiffsAbsent =
    diffCounts.length === 5 && diffCounts.every((item) => item === null);
  return (
    isRecord(reconciliation) &&
    typeof reconciliation.id === "string" &&
    ["open", "complete", "applied", "failed"].includes(
      String(reconciliation.status),
    ) &&
    isStringOrNull(reconciliation.failure_reason) &&
    isStringOrNull(reconciliation.next_cursor) &&
    isNonNegativeInteger(reconciliation.started_at_ms) &&
    isNonNegativeInteger(reconciliation.checkpoint_at_ms) &&
    reconciliation.checkpoint_at_ms >= reconciliation.started_at_ms &&
    isNonNegativeIntegerOrNull(reconciliation.finished_at_ms) &&
    (reconciliation.finished_at_ms === null ||
      reconciliation.finished_at_ms >= reconciliation.started_at_ms) &&
    isNonNegativeInteger(reconciliation.pages_completed) &&
    isNonNegativeInteger(reconciliation.events_seen) &&
    isNonNegativeInteger(reconciliation.groups_staged) &&
    isNonNegativeInteger(reconciliation.rejected_count) &&
    isNonNegativeInteger(reconciliation.duration_ms) &&
    isNonNegativeInteger(reconciliation.observations_count) &&
    isNonNegativeInteger(reconciliation.baseline_count) &&
    isNonNegativeIntegerOrNull(reconciliation.added_count) &&
    isNonNegativeIntegerOrNull(reconciliation.changed_count) &&
    isNonNegativeIntegerOrNull(reconciliation.closed_count) &&
    isNonNegativeIntegerOrNull(reconciliation.unchanged_count) &&
    isNonNegativeIntegerOrNull(reconciliation.applied_rejected_count) &&
    (allDiffsPresent || allDiffsAbsent) &&
    (reconciliation.status === "applied") === allDiffsPresent
  );
}

function isIncident(value: unknown): boolean {
  if (!isRecord(value)) return false;
  return (
    typeof value.incident_id === "string" &&
    typeof value.sequence === "number" &&
    typeof value.scope === "string" &&
    typeof value.kind === "string" &&
    [
      "detected",
      "classified",
      "contained",
      "recovering",
      "verified",
      "escalated",
    ].includes(String(value.state)) &&
    isNonNegativeInteger(value.detected_at_ms) &&
    typeof value.occurred_at_ms === "number" &&
    isNonNegativeInteger(value.lifecycle_age_ms) &&
    (value.action === null ||
      [
        "classify-producer-failure",
        "operator-intervention",
        "restart-producer",
        "retry-producer",
      ].includes(String(value.action))) &&
    isNonNegativeIntegerOrNull(value.retry_count) &&
    isNonNegativeIntegerOrNull(value.next_retry_at_ms) &&
    isNonNegativeIntegerOrNull(value.recovery_occurred_at_ms) &&
    (value.recovery_start_evidence === null ||
      isRecord(value.recovery_start_evidence)) &&
    (value.history_floor === null ||
      (isRecord(value.history_floor) &&
        isNonNegativeInteger(value.history_floor.through_event_id) &&
        isNonNegativeInteger(value.history_floor.compacted_event_count))) &&
    value.notification_delivery_tracked === false &&
    (value.diagnosis === null ||
      (isRecord(value.diagnosis) &&
        (value.diagnosis.severity === "p1" || value.diagnosis.severity === "p2") &&
        isPositiveInteger(value.diagnosis.reminder_interval_s) &&
        (value.diagnosis.impact === "feed-at-risk" ||
          value.diagnosis.impact === "feed-unavailable") &&
        (value.diagnosis.automatic_action === "retry-immediately" ||
          value.diagnosis.automatic_action === "retry-at-next-cadence" ||
          value.diagnosis.automatic_action === "retry-supervised-producer" ||
          value.diagnosis.automatic_action === "automatic-retries-exhausted") &&
        (value.diagnosis.next_action === "inspect-clob-and-child-io" ||
          value.diagnosis.next_action === "inspect-child-stderr" ||
          value.diagnosis.next_action === "inspect-producer-receipt-and-restart") &&
        (isPositiveInteger(value.diagnosis.deadline_s) ||
          value.diagnosis.deadline_s === null) &&
        isPositiveInteger(value.diagnosis.consecutive_failures) &&
        typeof value.diagnosis.last_success_age_s === "number" &&
        value.diagnosis.last_success_age_s >= 0 &&
        value.diagnosis.free_percent === null &&
        (value.diagnosis.failure_reason === null ||
          typeof value.diagnosis.failure_reason === "string")) ||
      (isRecord(value.diagnosis) &&
        (value.diagnosis.severity === "p1" || value.diagnosis.severity === "p2") &&
        isPositiveInteger(value.diagnosis.reminder_interval_s) &&
        value.diagnosis.impact === "storage-exhaustion-risk" &&
        value.diagnosis.automatic_action === "reclaim-bounded-history" &&
        value.diagnosis.next_action === "inspect-capacity-receipts" &&
        value.diagnosis.deadline_s === null &&
        isNonNegativeInteger(value.diagnosis.consecutive_failures) &&
        value.diagnosis.last_success_age_s === null &&
        typeof value.diagnosis.free_percent === "number" &&
        value.diagnosis.free_percent >= 0 &&
        value.diagnosis.free_percent <= 100 &&
        (value.diagnosis.failure_reason === null ||
          typeof value.diagnosis.failure_reason === "string")) ||
      (isRecord(value.diagnosis) &&
        (value.diagnosis.severity === "p1" || value.diagnosis.severity === "p2") &&
        isPositiveInteger(value.diagnosis.reminder_interval_s) &&
        value.diagnosis.impact === "market-map-stale" &&
        value.diagnosis.automatic_action === "retry-bounded-structure-child" &&
        value.diagnosis.next_action === "inspect-stage-checkpoint-and-child-budget" &&
        value.diagnosis.deadline_s === null &&
        isPositiveInteger(value.diagnosis.consecutive_failures) &&
        value.diagnosis.last_success_age_s === null &&
        value.diagnosis.free_percent === null &&
        typeof value.diagnosis.failure_reason === "string" &&
        isNonNegativeIntegerOrNull(value.diagnosis.elapsed_ms) &&
        isStringOrNull(value.diagnosis.last_stage) &&
        isPositiveInteger(value.diagnosis.cooperative_slice_budget_s) &&
        isPositiveInteger(value.diagnosis.child_hard_limit_s) &&
        value.diagnosis.cooperative_slice_budget_s <
          value.diagnosis.child_hard_limit_s)) &&
    isRecord(value.evidence)
  );
}

function isHealthCheck(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.componentId === "string" &&
    ["pass", "warn", "fail"].includes(String(value.status)) &&
    "observedValue" in value
  );
}

function isHealthEnvelope(value: unknown): value is PerceptionHealthEnvelope {
  return (
    isRecord(value) &&
    ["pass", "warn", "fail"].includes(String(value.status)) &&
    typeof value.releaseId === "string" &&
    typeof value.machineId === "string" &&
    typeof value.bootId === "string" &&
    isRecord(value.checks) &&
    Object.values(value.checks).every(
      (entries) => Array.isArray(entries) && entries.every(isHealthCheck),
    )
  );
}

function isIncidentsEnvelope(
  value: unknown,
): value is PerceptionIncidentsEnvelope {
  return (
    isRecord(value) &&
    value.status === "available" &&
    Array.isArray(value.items) &&
    value.items.every(isIncident) &&
    typeof value.limit === "number" &&
    isNonNegativeInteger(value.open_count) &&
    (value.next_before === null || typeof value.next_before === "string")
  );
}

function isResourceSample(value: unknown): value is PerceptionResourceSample {
  if (!isRecord(value)) return false;
  return (
    isNonNegativeInteger(value.candidate_count) &&
    isNumberOrNull(value.candidate_quote_p95_ms) &&
    (value.candidate_quote_p95_ms === null ||
      value.candidate_quote_p95_ms >= 0) &&
    isNonNegativeInteger(value.candidate_missing_quote_count) &&
    value.candidate_missing_quote_count <= value.candidate_count &&
    typeof value.candidate_worker_ok === "boolean" &&
    typeof value.discovery_worker_ok === "boolean" &&
    typeof value.reconciliation_running === "boolean" &&
    isPositiveInteger(value.previous_discovery_batch_limit) &&
    value.previous_discovery_batch_limit <= 100 &&
    isNonNegativeInteger(value.observed_at_ms)
  );
}

function isResourceDecision(
  value: unknown,
): value is PerceptionResourceDecision {
  if (!isRecord(value)) return false;
  return (
    ["normal", "protect-hot-path", "empty-candidate-exploration"].includes(
      String(value.mode),
    ) &&
    [
      "candidate-hot-path-pressure",
      "empty-candidate-exploration",
      "candidate-hot-path-fresh",
      "hysteresis-cooldown",
    ].includes(String(value.reason)) &&
    typeof value.reconciliation_enabled === "boolean" &&
    isPositiveInteger(value.discovery_batch_limit) &&
    value.discovery_batch_limit <= 100 &&
    isNonNegativeNumber(value.discovery_duty_multiplier) &&
    isNonNegativeNumber(value.normal_candidate_interval_multiplier) &&
    isNonNegativeNumber(value.high_candidate_interval_multiplier) &&
    value.http_preserved === true &&
    typeof value.health_claimed === "boolean" &&
    isPositiveInteger(value.previous_discovery_batch_limit) &&
    value.previous_discovery_batch_limit <= 100 &&
    isNonNegativeInteger(value.decided_at_ms) &&
    value.policy_version === "opportunity-resource-v1" &&
    isPositiveInteger(value.sequence) &&
    isPositiveInteger(value.source_sample_id) &&
    isPositiveInteger(value.hot_quote_age_ms) &&
    isNonNegativeInteger(value.cooldown_ms) &&
    isPositiveInteger(value.decision_ttl_ms) &&
    isNonNegativeInteger(value.valid_until_ms) &&
    value.valid_until_ms === value.decided_at_ms + value.decision_ttl_ms &&
    isNonNegativeInteger(value.mode_changed_at_ms) &&
    value.mode_changed_at_ms <= value.decided_at_ms
  );
}

export function isResourcesEnvelope(
  value: unknown,
): value is PerceptionResourcesEnvelope {
  if (
    !isRecord(value) ||
    value.status !== "available" ||
    (value.current !== null && !isResourceDecision(value.current)) ||
    !Array.isArray(value.items) ||
    !isPositiveInteger(value.limit) ||
    value.limit > 500 ||
    (value.next_before_sequence !== null &&
      !isPositiveInteger(value.next_before_sequence)) ||
    !(
      value.history_floor === null ||
      (isRecord(value.history_floor) &&
        isPositiveInteger(value.history_floor.through_sample_id) &&
        isPositiveInteger(value.history_floor.through_decision_id) &&
        isPositiveInteger(value.history_floor.through_sequence) &&
        value.history_floor.compacted_sample_count ===
          value.history_floor.through_sequence &&
        value.history_floor.compacted_decision_count ===
          value.history_floor.through_sequence)
    )
  ) {
    return false;
  }
  let previousSequence: number | null = null;
  for (const item of value.items) {
    if (
      !isRecord(item) ||
      !isResourceSample(item.sample) ||
      !isResourceDecision(item.decision) ||
      !isRecord(item.sample) ||
      !isRecord(item.decision) ||
      item.sample.observed_at_ms > item.decision.decided_at_ms ||
      (previousSequence !== null &&
        item.decision.sequence >= previousSequence)
    ) {
      return false;
    }
    previousSequence = item.decision.sequence as number;
  }
  if (
    value.items.length > value.limit ||
    (value.current === null && value.items.length !== 0) ||
    (value.current !== null &&
      value.items.length > 0 &&
      value.current.sequence < value.items[0].decision.sequence) ||
    (value.next_before_sequence !== null &&
      (value.items.length !== value.limit ||
        value.next_before_sequence !==
          value.items[value.items.length - 1].decision.sequence))
  ) {
    return false;
  }
  return true;
}

async function fetchAvailable<T>(
  path: string,
  signal: AbortSignal,
  validate: (value: unknown) => value is T,
): Promise<T> {
  const response = await fetch(`${PERCEPTION_BASE_URL.replace(/\/$/, "")}${path}`, {
    cache: "no-store",
    signal,
  });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new Error("invalid JSON");
  }
  if (!validate(body)) {
    throw new Error("invalid JSON contract");
  }
  return body;
}

function unavailable(error: unknown): PerceptionReadResult<never> {
  return {
    status: "unavailable",
    reason: error instanceof Error ? error.message : "transport unavailable",
  };
}

export function candidateEnvelopesAgree(
  status: PerceptionStatusEnvelope,
  currentOpportunities: PerceptionCurrentOpportunitiesEnvelope,
): boolean {
  return (
    status.candidate_authority_hash ===
      currentOpportunities.candidate_authority_hash &&
    status.opportunities.status === "available" &&
    status.opportunities.count ===
      currentOpportunities.current_opportunity_count &&
    (currentOpportunities.current_opportunity_count >
      currentOpportunities.items.length) ===
      (currentOpportunities.next_after_group_id !== null)
  );
}

export async function readPerceptionOverview(): Promise<
  PerceptionReadResult<PerceptionOverview>
> {
  const signal = AbortSignal.timeout(3000);
  try {
    const [
      health,
      status,
      currentOpportunities,
      groups,
      discovery,
      reconciliation,
      incidents,
      resources,
    ] =
      await Promise.all([
        fetchAvailable<PerceptionHealthEnvelope>(
          "/healthz",
          signal,
          isHealthEnvelope,
        ),
        fetchAvailable<PerceptionStatusEnvelope>(
          "/perception/status",
          signal,
          isStatusEnvelope,
        ),
        fetchAvailable<PerceptionCurrentOpportunitiesEnvelope>(
          `/perception/opportunities?limit=${OPPORTUNITY_LIMIT}`,
          signal,
          isCurrentOpportunitiesEnvelope,
        ),
        fetchAvailable<PerceptionGroupsEnvelope>(
          `/perception/groups?limit=${GROUP_LIMIT}`,
          signal,
          isGroupsEnvelope,
        ),
        fetchAvailable<PerceptionDiscoveryEnvelope>(
          "/perception/discovery",
          signal,
          isDiscoveryEnvelope,
        ),
        fetchAvailable<PerceptionReconciliationEnvelope>(
          "/perception/reconciliation",
          signal,
          isReconciliationEnvelope,
        ),
        fetchAvailable<PerceptionIncidentsEnvelope>(
          `/perception/incidents?limit=${INCIDENT_LIMIT}`,
          signal,
          isIncidentsEnvelope,
        ),
        fetchAvailable<PerceptionResourcesEnvelope>(
          `/perception/resources?limit=${RESOURCE_LIMIT}`,
          signal,
          isResourcesEnvelope,
        ),
      ]);
    if (!candidateEnvelopesAgree(status, currentOpportunities)) {
      throw new Error("candidate read snapshots changed");
    }
    if (
      (resources.current === null && resources.items.length !== 0) ||
      (resources.current !== null &&
        (resources.items.length === 0 ||
          resources.current.sequence !==
            resources.items[0].decision.sequence))
    ) {
      throw new Error("resource read snapshot changed");
    }
    return {
      status: "available",
      data: {
        health,
        status,
        currentOpportunities,
        groups,
        discovery,
        reconciliation,
        incidents,
        resources,
      },
    };
  } catch (error) {
    return unavailable(error);
  }
}

export async function readPerceptionGroupHistory(
  groupId: string,
): Promise<PerceptionReadResult<PerceptionGroupDetail>> {
  const signal = AbortSignal.timeout(3000);
  const encodedGroupId = encodeURIComponent(groupId);
  try {
    const timeline = await fetchAvailable<PerceptionGroupTimelineEnvelope>(
      `/perception/groups/${encodedGroupId}/timeline?limit=${HISTORY_LIMIT}`,
      signal,
      (value): value is PerceptionGroupTimelineEnvelope =>
        isGroupTimelineEnvelope(value, groupId),
    );
    return {
      status: "available",
      data: { timeline },
    };
  } catch (error) {
    return unavailable(error);
  }
}
