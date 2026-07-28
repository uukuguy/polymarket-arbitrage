import type {
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
const INCIDENT_LIMIT = 500;
const HISTORY_LIMIT = 100;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isStringOrNull(value: unknown): value is string | null {
  return typeof value === "string" || value === null;
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

function isAdmissionProof(value: unknown): boolean {
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

function isStatusEnvelope(value: unknown): value is PerceptionStatusEnvelope {
  if (!isRecord(value) || value.status !== "available") return false;
  const opportunities = value.opportunities;
  return (
    isRecord(opportunities) &&
    ["available", "unavailable"].includes(String(opportunities.status)) &&
    isNumberOrNull(opportunities.count) &&
    typeof opportunities.reason === "string" &&
    typeof value.open_incident_count === "number"
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
    typeof discovery.candidate_start_ready === "boolean" &&
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
    isAdmissionProof(discovery.admission_proof)
  );
}

export function isReconciliationEnvelope(
  value: unknown,
): value is PerceptionReconciliationEnvelope {
  if (!isRecord(value) || value.status !== "available") return false;
  if (value.reconciliation === null) return true;
  const reconciliation = value.reconciliation;
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
    isNonNegativeIntegerOrNull(reconciliation.applied_rejected_count)
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

export async function readPerceptionOverview(): Promise<
  PerceptionReadResult<PerceptionOverview>
> {
  const signal = AbortSignal.timeout(3000);
  try {
    const [status, groups, discovery, reconciliation, incidents] =
      await Promise.all([
        fetchAvailable<PerceptionStatusEnvelope>(
          "/perception/status",
          signal,
          isStatusEnvelope,
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
    return {
      status: "available",
      data: { status, groups, discovery, reconciliation, incidents },
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
