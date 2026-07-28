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
  return typeof value === "number" || value === null;
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

function isDiscoveryEnvelope(
  value: unknown,
): value is PerceptionDiscoveryEnvelope {
  if (!isRecord(value) || value.status !== "available") return false;
  if (value.discovery === null) return true;
  const discovery = value.discovery;
  return (
    isRecord(discovery) &&
    isStringOrNull(discovery.next_cursor) &&
    typeof discovery.completed === "boolean" &&
    isNumberOrNull(discovery.last_started_at_ms) &&
    isNumberOrNull(discovery.last_finished_at_ms) &&
    typeof discovery.page_event_count === "number" &&
    typeof discovery.groups_seen === "number" &&
    typeof discovery.promoted_count === "number" &&
    isRecord(discovery.queue_depth_by_class) &&
    Object.values(discovery.queue_depth_by_class).every(
      (depth) => typeof depth === "number",
    ) &&
    isNumberOrNull(discovery.oldest_visit_age_ms) &&
    typeof discovery.promotion_queue_depth === "number" &&
    typeof discovery.outstanding_admitted_count === "number" &&
    typeof discovery.candidate_start_ready === "boolean"
  );
}

function isReconciliationEnvelope(
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
    typeof reconciliation.started_at_ms === "number" &&
    typeof reconciliation.checkpoint_at_ms === "number" &&
    isNumberOrNull(reconciliation.finished_at_ms) &&
    typeof reconciliation.pages_completed === "number" &&
    typeof reconciliation.events_seen === "number" &&
    typeof reconciliation.groups_staged === "number" &&
    typeof reconciliation.rejected_count === "number"
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
