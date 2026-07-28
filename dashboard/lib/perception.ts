import type {
  PerceptionCurrentOpportunitiesEnvelope,
  PerceptionDiscoveryEnvelope,
  PerceptionGroupDetail,
  PerceptionGroupHistoryEnvelope,
  PerceptionGroupsEnvelope,
  PerceptionIncidentsEnvelope,
  PerceptionOverview,
  PerceptionReadResult,
  PerceptionReconciliationEnvelope,
  PerceptionStatusEnvelope,
} from "@/lib/types";

const PERCEPTION_BASE_URL =
  process.env.POLYARB_L1_URL ?? "https://polyarb-l1.fly.dev";
const GROUP_LIMIT = 100;
const OPPORTUNITY_LIMIT = 100;
const INCIDENT_LIMIT = 500;
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
    typeof value.occurred_at_ms === "number" &&
    isRecord(value.evidence)
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
    typeof value.limit === "number"
  );
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
      status,
      currentOpportunities,
      groups,
      discovery,
      reconciliation,
      incidents,
    ] =
      await Promise.all([
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
      ]);
    if (!candidateEnvelopesAgree(status, currentOpportunities)) {
      throw new Error("candidate read snapshots changed");
    }
    return {
      status: "available",
      data: {
        status,
        currentOpportunities,
        groups,
        discovery,
        reconciliation,
        incidents,
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
    const [history, incidents] = await Promise.all([
      fetchAvailable<PerceptionGroupHistoryEnvelope>(
        `/perception/groups/${encodedGroupId}/history?limit=${HISTORY_LIMIT}`,
        signal,
        (value): value is PerceptionGroupHistoryEnvelope =>
          isGroupHistoryEnvelope(value, groupId),
      ),
      fetchAvailable<PerceptionIncidentsEnvelope>(
        `/perception/incidents?limit=${INCIDENT_LIMIT}`,
        signal,
        isIncidentsEnvelope,
      ),
    ]);
    return {
      status: "available",
      data: { history, incidents },
    };
  } catch (error) {
    return unavailable(error);
  }
}
