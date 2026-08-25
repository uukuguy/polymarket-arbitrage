const CONTROL_PLANE_BASE_URL =
  process.env.POLYARB_CONTROL_API_URL ?? "https://polyarb-control-api.fly.dev";
const CONTROL_PLANE_SAMPLE_LIMIT = 20;
const UNAVAILABLE_REASON = "control-plane-read-unavailable";
const RUNTIME_CONTROLLER_ID = "m1-runtime-reconciler";

export type RuntimeState = "healthy" | "degraded" | "recovering" | "critical";
export type RuntimeControllerState = "healthy" | "critical";
export type ActiveTaskState = "active" | "suspect" | "recovering";
export type IncidentState = "open" | "acknowledged" | "resolved";
export type IncidentSeverity = "info" | "warning" | "critical";
export type IncidentTransition =
  | "detected"
  | "recovery-started"
  | "recovery-attempted"
  | "recovered"
  | "resolved"
  | "escalated";
export type QualificationImpact = "breaking" | "contained" | "qualified" | "delayed";
export type RecoveryActionState =
  | "pending"
  | "running"
  | "succeeded"
  | "failed"
  | "stale-noop";
export type RecoveryActionRawState = "pending" | "running" | "completed";
export type RecoveryActionResult =
  | "succeeded"
  | "failed"
  | "stale-noop"
  | "disabled-action";
export type QualificationState =
  | "accumulating"
  | "invalidated"
  | "recovering"
  | "qualified";

export type RuntimeEvent = {
  incident_key: string;
  severity: IncidentSeverity;
  summary: string;
  kind: "detected" | "recovered";
  occurred_at: string;
  detail: { failures: string[]; source?: string };
};

export type ActiveRuntimeIncident = {
  incident_key: string;
  severity: IncidentSeverity;
  summary: string;
  opened_at: string;
  source: string;
  failures: string[];
};

export type RuntimeControllerView =
  | {
      status: "unavailable";
      reason: "missing-controller";
      controller_id: typeof RUNTIME_CONTROLLER_ID;
      owner_id: null;
      epoch: null;
      claimed_at: null;
      last_tick_at: null;
      lease_expires_at: null;
      lease_active: false;
      lease_age_seconds: null;
      lease_overdue_seconds: null;
    }
  | {
      status: RuntimeControllerState;
      controller_id: typeof RUNTIME_CONTROLLER_ID;
      owner_id: string;
      epoch: number;
      claimed_at: string;
      last_tick_at: string;
      lease_expires_at: string;
      lease_active: boolean;
      lease_age_seconds: number;
      lease_overdue_seconds: number;
    };

export type ActiveTask = {
  job_key: string;
  attempt_id: string;
  job_type: RuntimeJobType;
  worker_id: string;
  lease_epoch: number;
  stage: RuntimeStage;
  recovery_state: ActiveTaskState;
  progress: { current: number; total: number | null };
  started_at: string;
  last_heartbeat_at: string;
  last_progress_at: string;
  lease_deadline_at: string;
  heartbeat_deadline_at: string;
  progress_deadline_at: string;
  attempt_deadline_at: string;
  heartbeat_age_seconds: number;
  progress_age_seconds: number;
  lease_overdue_seconds: number;
  attempt_overdue_seconds: number;
};

export type RuntimeIncidentTransition = {
  kind: IncidentTransition;
  occurred_at: string;
  age_seconds: number;
  reason_code?: string;
  qualification_impact?: QualificationImpact;
};

export type RuntimeIncident = {
  incident_key: string;
  component: string;
  severity: IncidentSeverity;
  state: IncidentState;
  summary: string;
  opened_at: string;
  updated_at: string;
  age_seconds: number;
  transition: IncidentTransition | null;
  transitions: RuntimeIncidentTransition[];
};

export type RecoveryAction = {
  action_id: string;
  incident_key: string | null;
  target_type: RecoveryActionTargetType;
  target_id: string;
  action_type: RecoveryActionType;
  raw_state: RecoveryActionRawState;
  state: RecoveryActionState;
  result_code: RecoveryActionResult | null;
  expected_controller_epoch: number;
  expected_attempt_id: string;
  expected_lease_epoch: number;
  requested_at: string;
  started_at: string | null;
  finished_at: string | null;
  next_allowed_at: string;
  worker_id: string | null;
  worker_epoch: number;
  worker_lease_expires_at: string | null;
};

export type QualificationView = {
  state: QualificationState;
  epoch_id: string | null;
  started_at: string | null;
  eligible_seconds: number;
  required_seconds: number | null;
  max_gap_seconds: number | null;
  last_fact_at: string | null;
  last_fact_age_seconds: number | null;
  last_breaker: {
    observed_at: string;
    reason: string;
    fact_id: string;
  } | null;
  policy_version: string | null;
  release_id: string | null;
  config_id: string | null;
  role_identity: string[];
  certificate: {
    certificate_id: string;
    certificate_digest: string;
    evidence_digest: string;
    qualified_at: string;
    created_at: string;
  } | null;
};

type CloudUsage = {
  budget_day: string;
  used_bytes: number;
  daily_budget_bytes: number | null;
  threshold_percent: number;
  latest_observation: {
    source: string;
    operation: string;
    bytes_received: number;
    observed_at: string;
  } | null;
};

export type ControlPlaneRead =
  | { status: "unavailable"; reason: string }
  | {
      status: "available";
      job_counts: Record<string, number>;
      open_incidents: Array<{
        incident_key: string;
        component: string;
        severity: IncidentSeverity;
        summary: string;
      }>;
      runtime_watchdog: {
        current: ActiveRuntimeIncident | null;
        recent_events: RuntimeEvent[];
      };
      soak_evidence: {
        latest_run_id: string;
        latest_observed_at: string;
      } | null;
      cloud_usage: CloudUsage;
      runtime_controller: RuntimeControllerView;
      active_tasks: { items: ActiveTask[]; total: number };
      runtime_incidents: { items: RuntimeIncident[]; total: number };
      recovery_actions: { items: RecoveryAction[]; total: number };
      qualification: QualificationView;
    };

type RuntimeJobType =
  | "structure-fetch"
  | "structure-materialize"
  | "structure-normalize"
  | "structure-certify"
  | "quote-admit"
  | "quote-batch"
  | "quote-certify"
  | "opportunity-certify";

type RuntimeStage =
  | "started"
  | "fetch-page"
  | "validate-page"
  | "upload-page"
  | "commit-page"
  | "read-page-receipts"
  | "build-bundle"
  | "upload-bundle"
  | "commit-bundle"
  | "read-range"
  | "normalize-range"
  | "upload-range"
  | "commit-range"
  | "verify-parity"
  | "build-manifest"
  | "upload-manifest"
  | "commit-certification"
  | "read-manifest"
  | "read-shards"
  | "build-batches"
  | "upload-batches"
  | "commit-admission"
  | "read-input"
  | "fetch-books"
  | "build-artifact"
  | "upload-artifact"
  | "commit-receipt"
  | "verify-batches"
  | "publish-pointer"
  | "read-current-quote"
  | "compute-opportunities"
  | "upload-projection"
  | "publish-opportunity";

type RecoveryActionTargetType = "job" | "circuit" | "worker-process" | "machine";
type RecoveryActionType =
  | "heartbeat-job"
  | "cancel-job"
  | "retry-job"
  | "reclaim-job"
  | "probe-circuit"
  | "restart-worker-process"
  | "restart-machine";

const runtimeStagesByJob: Record<RuntimeJobType, readonly RuntimeStage[]> = {
  "structure-fetch": ["fetch-page", "validate-page", "upload-page", "commit-page"],
  "structure-materialize": [
    "read-page-receipts",
    "build-bundle",
    "upload-bundle",
    "commit-bundle",
  ],
  "structure-normalize": ["read-range", "normalize-range", "upload-range", "commit-range"],
  "structure-certify": [
    "verify-parity",
    "build-manifest",
    "upload-manifest",
    "commit-certification",
  ],
  "quote-admit": [
    "read-manifest",
    "read-shards",
    "build-batches",
    "upload-batches",
    "commit-admission",
  ],
  "quote-batch": ["read-input", "fetch-books", "build-artifact", "upload-artifact", "commit-receipt"],
  "quote-certify": ["verify-batches", "publish-pointer"],
  "opportunity-certify": [
    "read-current-quote",
    "compute-opportunities",
    "upload-projection",
    "publish-opportunity",
  ],
};

const runtimeStates = ["healthy", "degraded", "recovering", "critical"] as const;
const runtimeControllerStates = ["healthy", "critical"] as const;
const activeTaskStates = ["active", "suspect", "recovering"] as const;
const incidentStates = ["open", "acknowledged", "resolved"] as const;
const incidentSeverities = ["info", "warning", "critical"] as const;
const incidentTransitions = [
  "detected",
  "recovery-started",
  "recovery-attempted",
  "recovered",
  "resolved",
  "escalated",
] as const;
const qualificationImpacts = ["breaking", "contained", "qualified", "delayed"] as const;
const actionTargetTypes = ["job", "circuit", "worker-process", "machine"] as const;
const actionTypes = [
  "heartbeat-job",
  "cancel-job",
  "retry-job",
  "reclaim-job",
  "probe-circuit",
  "restart-worker-process",
  "restart-machine",
] as const;
const actionRawStates = ["pending", "running", "completed"] as const;
const actionResults = ["succeeded", "failed", "stale-noop", "disabled-action"] as const;
const qualificationStates = [
  "accumulating",
  "invalidated",
  "recovering",
  "qualified",
] as const;

function unavailable(): ControlPlaneRead {
  return { status: "unavailable", reason: UNAVAILABLE_REASON };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isString(value: unknown): value is string {
  return typeof value === "string";
}

function isStringOrNull(value: unknown): value is string | null {
  return typeof value === "string" || value === null;
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function isNonNegativeNumber(value: unknown): value is number {
  return isFiniteNumber(value) && value >= 0;
}

function isNonNegativeInteger(value: unknown): value is number {
  return isNonNegativeNumber(value) && Number.isSafeInteger(value);
}

function isPositiveInteger(value: unknown): value is number {
  return isNonNegativeInteger(value) && value > 0;
}

function isValidCalendarDate(year: number, month: number, day: number): boolean {
  if (month < 1 || month > 12) return false;
  const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
  return day >= 1 && day <= daysInMonth;
}

function isIsoTimestamp(value: unknown): value is string {
  if (!isString(value)) return false;
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:\d{2})$/.exec(
      value,
    );
  if (!match) return false;
  const [, yearText, monthText, dayText, hourText, minuteText, secondText, , zone] =
    match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  if (
    !isValidCalendarDate(year, month, day) ||
    hour > 23 ||
    minute > 59 ||
    second > 59
  ) {
    return false;
  }
  if (zone !== "Z") {
    const zoneHour = Number(zone.slice(1, 3));
    const zoneMinute = Number(zone.slice(4, 6));
    if (zoneHour > 23 || zoneMinute > 59) return false;
  }
  return Number.isFinite(Date.parse(value));
}

function isBudgetDay(value: unknown): value is string {
  if (!isString(value)) return false;
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return false;
  return isValidCalendarDate(Number(match[1]), Number(match[2]), Number(match[3]));
}

function isDateString(value: unknown): value is string {
  return isIsoTimestamp(value);
}

function isDateStringOrNull(value: unknown): value is string | null {
  return value === null || isDateString(value);
}

function isLiteral<T extends readonly string[]>(value: unknown, values: T): value is T[number] {
  return typeof value === "string" && values.includes(value);
}

function validateBoundedArray<T>(
  value: unknown,
  validator: (item: unknown) => T | null,
): T[] | null {
  if (!Array.isArray(value) || value.length > CONTROL_PLANE_SAMPLE_LIMIT) return null;
  const items: T[] = [];
  for (const item of value) {
    const validated = validator(item);
    if (validated === null) return null;
    items.push(validated);
  }
  return items;
}

function validateStringArray(value: unknown): string[] | null {
  return validateBoundedArray(value, (item) => (isString(item) ? item : null));
}

function validateTotal(total: unknown, itemCount: number): number | null {
  if (!isNonNegativeInteger(total) || total < itemCount) return null;
  return total;
}

function validateJobCounts(value: unknown): Record<string, number> | null {
  if (!isRecord(value)) return null;
  const counts: Record<string, number> = {};
  for (const [key, count] of Object.entries(value)) {
    if (!key || !isNonNegativeInteger(count)) return null;
    counts[key] = count;
  }
  return counts;
}

function validateOpenIncident(value: unknown): {
  incident_key: string;
  component: string;
  severity: IncidentSeverity;
  summary: string;
} | null {
  if (!isRecord(value)) return null;
  if (
    !isString(value.incident_key) ||
    !isString(value.component) ||
    !isLiteral(value.severity, incidentSeverities) ||
    !isString(value.summary)
  ) {
    return null;
  }
  return {
    incident_key: value.incident_key,
    component: value.component,
    severity: value.severity,
    summary: value.summary,
  };
}

function validateRuntimeEvent(value: unknown): RuntimeEvent | null {
  if (!isRecord(value) || (value.kind !== "detected" && value.kind !== "recovered")) {
    return null;
  }
  if (
    !isString(value.incident_key) ||
    !isLiteral(value.severity, incidentSeverities) ||
    !isString(value.summary) ||
    !isDateString(value.occurred_at) ||
    !isRecord(value.detail)
  ) {
    return null;
  }
  const failures = validateStringArray(value.detail.failures);
  if (failures === null) return null;
  if (value.detail.source !== undefined && !isString(value.detail.source)) return null;
  return {
    incident_key: value.incident_key,
    severity: value.severity,
    summary: value.summary,
    kind: value.kind,
    occurred_at: value.occurred_at,
    detail: {
      failures,
      ...(value.detail.source === undefined ? {} : { source: value.detail.source }),
    },
  };
}

function validateActiveRuntimeIncident(value: unknown): ActiveRuntimeIncident | null {
  if (!isRecord(value)) return null;
  const failures = validateStringArray(value.failures);
  if (
    failures === null ||
    !isString(value.incident_key) ||
    !isLiteral(value.severity, incidentSeverities) ||
    !isString(value.summary) ||
    !isDateString(value.opened_at) ||
    !isString(value.source)
  ) {
    return null;
  }
  return {
    incident_key: value.incident_key,
    severity: value.severity,
    summary: value.summary,
    opened_at: value.opened_at,
    source: value.source,
    failures,
  };
}

function validateRuntimeWatchdog(value: unknown): {
  current: ActiveRuntimeIncident | null;
  recent_events: RuntimeEvent[];
} | null {
  if (!isRecord(value)) return null;
  const current =
    value.current === null ? null : validateActiveRuntimeIncident(value.current);
  const recentEvents = validateBoundedArray(value.recent_events, validateRuntimeEvent);
  if (current === null && value.current !== null) return null;
  if (recentEvents === null) return null;
  return { current, recent_events: recentEvents };
}

function validateRuntimeController(value: unknown): RuntimeControllerView | null {
  if (!isRecord(value) || value.controller_id !== RUNTIME_CONTROLLER_ID) return null;
  if (value.status === "unavailable") {
    if (
      value.reason !== "missing-controller" ||
      value.owner_id !== null ||
      value.epoch !== null ||
      value.claimed_at !== null ||
      value.last_tick_at !== null ||
      value.lease_expires_at !== null ||
      value.lease_active !== false ||
      value.lease_age_seconds !== null ||
      value.lease_overdue_seconds !== null
    ) {
      return null;
    }
    return {
      status: "unavailable",
      reason: "missing-controller",
      controller_id: RUNTIME_CONTROLLER_ID,
      owner_id: null,
      epoch: null,
      claimed_at: null,
      last_tick_at: null,
      lease_expires_at: null,
      lease_active: false,
      lease_age_seconds: null,
      lease_overdue_seconds: null,
    };
  }
  if (
    !isLiteral(value.status, runtimeControllerStates) ||
    !isString(value.owner_id) ||
    !isPositiveInteger(value.epoch) ||
    !isDateString(value.claimed_at) ||
    !isDateString(value.last_tick_at) ||
    !isDateString(value.lease_expires_at) ||
    typeof value.lease_active !== "boolean" ||
    !isNonNegativeNumber(value.lease_age_seconds) ||
    !isNonNegativeNumber(value.lease_overdue_seconds)
  ) {
    return null;
  }
  if (
    (value.status === "healthy" &&
      (!value.lease_active || value.lease_overdue_seconds !== 0)) ||
    (value.status === "critical" && value.lease_active)
  ) {
    return null;
  }
  return {
    status: value.status,
    controller_id: RUNTIME_CONTROLLER_ID,
    owner_id: value.owner_id,
    epoch: value.epoch,
    claimed_at: value.claimed_at,
    last_tick_at: value.last_tick_at,
    lease_expires_at: value.lease_expires_at,
    lease_active: value.lease_active,
    lease_age_seconds: value.lease_age_seconds,
    lease_overdue_seconds: value.lease_overdue_seconds,
  };
}

function isRuntimeJobType(value: unknown): value is RuntimeJobType {
  return isString(value) && Object.hasOwn(runtimeStagesByJob, value);
}

function isRuntimeStage(jobType: RuntimeJobType, value: unknown): value is RuntimeStage {
  return (
    value === "started" ||
    (isString(value) && runtimeStagesByJob[jobType].includes(value as RuntimeStage))
  );
}

function validateActiveTask(value: unknown): ActiveTask | null {
  if (!isRecord(value) || !isRuntimeJobType(value.job_type)) return null;
  if (
    !isString(value.job_key) ||
    !isString(value.attempt_id) ||
    !isString(value.worker_id) ||
    !isPositiveInteger(value.lease_epoch) ||
    !isRuntimeStage(value.job_type, value.stage) ||
    !isLiteral(value.recovery_state, activeTaskStates) ||
    !isRecord(value.progress) ||
    !isNonNegativeInteger(value.progress.current) ||
    !(value.progress.total === null || isNonNegativeInteger(value.progress.total)) ||
    (value.progress.total !== null && value.progress.current > value.progress.total) ||
    !isDateString(value.started_at) ||
    !isDateString(value.last_heartbeat_at) ||
    !isDateString(value.last_progress_at) ||
    !isDateString(value.lease_deadline_at) ||
    !isDateString(value.heartbeat_deadline_at) ||
    !isDateString(value.progress_deadline_at) ||
    !isDateString(value.attempt_deadline_at) ||
    !isNonNegativeNumber(value.heartbeat_age_seconds) ||
    !isNonNegativeNumber(value.progress_age_seconds) ||
    !isNonNegativeNumber(value.lease_overdue_seconds) ||
    !isNonNegativeNumber(value.attempt_overdue_seconds)
  ) {
    return null;
  }
  return {
    job_key: value.job_key,
    attempt_id: value.attempt_id,
    job_type: value.job_type,
    worker_id: value.worker_id,
    lease_epoch: value.lease_epoch,
    stage: value.stage,
    recovery_state: value.recovery_state,
    progress: { current: value.progress.current, total: value.progress.total },
    started_at: value.started_at,
    last_heartbeat_at: value.last_heartbeat_at,
    last_progress_at: value.last_progress_at,
    lease_deadline_at: value.lease_deadline_at,
    heartbeat_deadline_at: value.heartbeat_deadline_at,
    progress_deadline_at: value.progress_deadline_at,
    attempt_deadline_at: value.attempt_deadline_at,
    heartbeat_age_seconds: value.heartbeat_age_seconds,
    progress_age_seconds: value.progress_age_seconds,
    lease_overdue_seconds: value.lease_overdue_seconds,
    attempt_overdue_seconds: value.attempt_overdue_seconds,
  };
}

function validateRuntimeIncidentTransition(
  value: unknown,
): RuntimeIncidentTransition | null {
  if (!isRecord(value)) return null;
  if (
    !isLiteral(value.kind, incidentTransitions) ||
    !isDateString(value.occurred_at) ||
    !isNonNegativeNumber(value.age_seconds) ||
    (value.reason_code !== undefined && !isString(value.reason_code)) ||
    (value.qualification_impact !== undefined &&
      !isLiteral(value.qualification_impact, qualificationImpacts))
  ) {
    return null;
  }
  return {
    kind: value.kind,
    occurred_at: value.occurred_at,
    age_seconds: value.age_seconds,
    ...(value.reason_code === undefined ? {} : { reason_code: value.reason_code }),
    ...(value.qualification_impact === undefined
      ? {}
      : { qualification_impact: value.qualification_impact }),
  };
}

function validateRuntimeIncident(value: unknown): RuntimeIncident | null {
  if (!isRecord(value)) return null;
  const transitions = validateBoundedArray(
    value.transitions,
    validateRuntimeIncidentTransition,
  );
  if (
    transitions === null ||
    !isString(value.incident_key) ||
    !isString(value.component) ||
    !isLiteral(value.severity, incidentSeverities) ||
    !isLiteral(value.state, incidentStates) ||
    !isString(value.summary) ||
    !isDateString(value.opened_at) ||
    !isDateString(value.updated_at) ||
    !isNonNegativeNumber(value.age_seconds) ||
    !(
      value.transition === null ||
      isLiteral(value.transition, incidentTransitions)
    )
  ) {
    return null;
  }
  return {
    incident_key: value.incident_key,
    component: value.component,
    severity: value.severity,
    state: value.state,
    summary: value.summary,
    opened_at: value.opened_at,
    updated_at: value.updated_at,
    age_seconds: value.age_seconds,
    transition: value.transition,
    transitions,
  };
}

function normalizeRecoveryActionState(
  rawState: RecoveryActionRawState,
  resultCode: RecoveryActionResult | null,
): RecoveryActionState | null {
  if (rawState === "pending" || rawState === "running") {
    return resultCode === null ? rawState : null;
  }
  if (resultCode === "succeeded" || resultCode === "failed" || resultCode === "stale-noop") {
    return resultCode;
  }
  return resultCode === "disabled-action" ? "failed" : null;
}

function validateRecoveryActionLifecycle(
  value: Record<string, unknown>,
  rawState: RecoveryActionRawState,
  resultCode: RecoveryActionResult | null,
): boolean {
  if (rawState === "pending") {
    return (
      resultCode === null &&
      value.started_at === null &&
      value.finished_at === null &&
      value.worker_id === null &&
      value.worker_epoch === 0 &&
      value.worker_lease_expires_at === null
    );
  }
  if (rawState === "running") {
    return (
      resultCode === null &&
      isDateString(value.started_at) &&
      value.finished_at === null &&
      isString(value.worker_id) &&
      isPositiveInteger(value.worker_epoch) &&
      isDateString(value.worker_lease_expires_at)
    );
  }
  if (resultCode === null || !isDateString(value.finished_at)) return false;
  const isClaimedTerminal =
    isDateString(value.started_at) &&
    isString(value.worker_id) &&
    isPositiveInteger(value.worker_epoch) &&
    isDateString(value.worker_lease_expires_at);
  const isImmediateTerminal =
    value.started_at === null &&
    value.worker_id === null &&
    value.worker_epoch === 0 &&
    value.worker_lease_expires_at === null &&
    value.finished_at === value.requested_at;
  if (resultCode === "succeeded" || resultCode === "failed") {
    return isClaimedTerminal;
  }
  return isClaimedTerminal || isImmediateTerminal;
}

function validateRecoveryAction(value: unknown): RecoveryAction | null {
  if (!isRecord(value)) return null;
  const resultCode =
    value.result_code === null
      ? null
      : isLiteral(value.result_code, actionResults)
        ? value.result_code
        : undefined;
  if (
    resultCode === undefined ||
    !isString(value.action_id) ||
    !isStringOrNull(value.incident_key) ||
    !isLiteral(value.target_type, actionTargetTypes) ||
    !isString(value.target_id) ||
    !isLiteral(value.action_type, actionTypes) ||
    !isLiteral(value.state, actionRawStates) ||
    !isPositiveInteger(value.expected_controller_epoch) ||
    !isString(value.expected_attempt_id) ||
    !isPositiveInteger(value.expected_lease_epoch) ||
    !isDateString(value.requested_at) ||
    !isDateStringOrNull(value.started_at) ||
    !isDateStringOrNull(value.finished_at) ||
    !isDateString(value.next_allowed_at) ||
    !isStringOrNull(value.worker_id) ||
    !isNonNegativeInteger(value.worker_epoch) ||
    !isDateStringOrNull(value.worker_lease_expires_at)
  ) {
    return null;
  }
  const state = normalizeRecoveryActionState(value.state, resultCode);
  if (
    state === null ||
    !validateRecoveryActionLifecycle(value, value.state, resultCode)
  ) {
    return null;
  }
  return {
    action_id: value.action_id,
    incident_key: value.incident_key,
    target_type: value.target_type,
    target_id: value.target_id,
    action_type: value.action_type,
    raw_state: value.state,
    state,
    result_code: resultCode,
    expected_controller_epoch: value.expected_controller_epoch,
    expected_attempt_id: value.expected_attempt_id,
    expected_lease_epoch: value.expected_lease_epoch,
    requested_at: value.requested_at,
    started_at: value.started_at,
    finished_at: value.finished_at,
    next_allowed_at: value.next_allowed_at,
    worker_id: value.worker_id,
    worker_epoch: value.worker_epoch,
    worker_lease_expires_at: value.worker_lease_expires_at,
  };
}

function validateQualification(value: unknown): QualificationView | null {
  if (!isRecord(value) || !isLiteral(value.state, qualificationStates)) return null;
  if (
    !isStringOrNull(value.epoch_id) ||
    !isDateStringOrNull(value.started_at) ||
    !isNonNegativeInteger(value.eligible_seconds) ||
    !(value.required_seconds === null || isNonNegativeInteger(value.required_seconds)) ||
    !(value.max_gap_seconds === null || isNonNegativeInteger(value.max_gap_seconds)) ||
    !isDateStringOrNull(value.last_fact_at) ||
    !(value.last_fact_age_seconds === null || isNonNegativeNumber(value.last_fact_age_seconds)) ||
    !isStringOrNull(value.policy_version) ||
    !isStringOrNull(value.release_id) ||
    !isStringOrNull(value.config_id)
  ) {
    return null;
  }
  const roleIdentity = validateStringArray(value.role_identity);
  if (roleIdentity === null) return null;
  const lastBreaker = validateLastBreaker(value.last_breaker);
  const certificate = validateCertificate(value.certificate);
  if (lastBreaker === undefined || certificate === undefined) return null;
  if (value.epoch_id === null) {
    if (
      value.state !== "accumulating" ||
      value.started_at !== null ||
      value.eligible_seconds !== 0 ||
      value.required_seconds !== null ||
      value.max_gap_seconds !== null ||
      value.last_fact_at !== null ||
      value.last_fact_age_seconds !== null ||
      value.policy_version !== null ||
      value.release_id !== null ||
      value.config_id !== null ||
      roleIdentity.length !== 0 ||
      certificate !== null
    ) {
      return null;
    }
  }
  const hasPolicyIdentity =
    value.policy_version !== null &&
    value.release_id !== null &&
    value.config_id !== null &&
    roleIdentity.length > 0;
  if (value.epoch_id !== null && !hasPolicyIdentity) return null;
  if (
    value.required_seconds !== null &&
    value.eligible_seconds > value.required_seconds
  ) {
    return null;
  }
  if (value.epoch_id !== null && value.started_at === null) return null;
  if (value.state === "qualified") {
    if (
      value.epoch_id === null ||
      value.required_seconds === null ||
      value.eligible_seconds !== value.required_seconds ||
      certificate === null
    ) {
      return null;
    }
  } else if (certificate !== null) {
    return null;
  }
  return {
    state: value.state,
    epoch_id: value.epoch_id,
    started_at: value.started_at,
    eligible_seconds: value.eligible_seconds,
    required_seconds: value.required_seconds,
    max_gap_seconds: value.max_gap_seconds,
    last_fact_at: value.last_fact_at,
    last_fact_age_seconds: value.last_fact_age_seconds,
    last_breaker: lastBreaker,
    policy_version: value.policy_version,
    release_id: value.release_id,
    config_id: value.config_id,
    role_identity: roleIdentity,
    certificate,
  };
}

function validateLastBreaker(value: unknown):
  | QualificationView["last_breaker"]
  | undefined {
  if (value === null) return null;
  if (!isRecord(value)) return undefined;
  if (
    !isDateString(value.observed_at) ||
    !isString(value.reason) ||
    !isString(value.fact_id)
  ) {
    return undefined;
  }
  return {
    observed_at: value.observed_at,
    reason: value.reason,
    fact_id: value.fact_id,
  };
}

function validateCertificate(value: unknown):
  | QualificationView["certificate"]
  | undefined {
  if (value === null) return null;
  if (!isRecord(value)) return undefined;
  if (
    !isString(value.certificate_id) ||
    !isString(value.certificate_digest) ||
    !isString(value.evidence_digest) ||
    !isDateString(value.qualified_at) ||
    !isDateString(value.created_at)
  ) {
    return undefined;
  }
  if (Date.parse(value.created_at) < Date.parse(value.qualified_at)) {
    return undefined;
  }
  return {
    certificate_id: value.certificate_id,
    certificate_digest: value.certificate_digest,
    evidence_digest: value.evidence_digest,
    qualified_at: value.qualified_at,
    created_at: value.created_at,
  };
}

function validateCollection<T>(
  value: unknown,
  validator: (item: unknown) => T | null,
): { items: T[]; total: number } | null {
  if (!isRecord(value)) return null;
  const items = validateBoundedArray(value.items, validator);
  if (items === null) return null;
  const total = validateTotal(value.total, items.length);
  return total === null ? null : { items, total };
}

function validateSoakEvidence(value: unknown): {
  latest_run_id: string;
  latest_observed_at: string;
} | null | undefined {
  if (value === null) return null;
  if (!isRecord(value)) return undefined;
  if (!isString(value.latest_run_id) || !isDateString(value.latest_observed_at)) {
    return undefined;
  }
  return {
    latest_run_id: value.latest_run_id,
    latest_observed_at: value.latest_observed_at,
  };
}

function validateCloudUsage(value: unknown): CloudUsage | null {
  if (!isRecord(value)) return null;
  const latest = validateCloudObservation(value.latest_observation);
  if (
    latest === undefined ||
    !isBudgetDay(value.budget_day) ||
    !isNonNegativeInteger(value.used_bytes) ||
    !(value.daily_budget_bytes === null || isNonNegativeInteger(value.daily_budget_bytes)) ||
    !isNonNegativeInteger(value.threshold_percent) ||
    value.threshold_percent > 100
  ) {
    return null;
  }
  return {
    budget_day: value.budget_day,
    used_bytes: value.used_bytes,
    daily_budget_bytes: value.daily_budget_bytes,
    threshold_percent: value.threshold_percent,
    latest_observation: latest,
  };
}

function validateCloudObservation(value: unknown):
  | {
      source: string;
      operation: string;
      bytes_received: number;
      observed_at: string;
    }
  | null
  | undefined {
  if (value === null) return null;
  if (!isRecord(value)) return undefined;
  if (
    !isString(value.source) ||
    !isString(value.operation) ||
    !isNonNegativeInteger(value.bytes_received) ||
    !isDateString(value.observed_at)
  ) {
    return undefined;
  }
  return {
    source: value.source,
    operation: value.operation,
    bytes_received: value.bytes_received,
    observed_at: value.observed_at,
  };
}

export function decodeControlPlaneRead(payload: unknown): ControlPlaneRead {
  if (!isRecord(payload)) return unavailable();
  if (payload.status === "unavailable") {
    return payload.reason === UNAVAILABLE_REASON ? unavailable() : unavailable();
  }
  if (payload.status !== "available") return unavailable();

  const jobCounts = validateJobCounts(payload.job_counts);
  const openIncidents = validateBoundedArray(
    payload.open_incidents,
    validateOpenIncident,
  );
  const runtimeWatchdog = validateRuntimeWatchdog(payload.runtime_watchdog);
  const soakEvidence = validateSoakEvidence(payload.soak_evidence);
  const cloudUsage = validateCloudUsage(payload.cloud_usage);
  const runtimeController = validateRuntimeController(payload.runtime_controller);
  const activeTasks = validateCollection(payload.active_tasks, validateActiveTask);
  const runtimeIncidents = validateCollection(
    payload.runtime_incidents,
    validateRuntimeIncident,
  );
  const recoveryActions = validateCollection(
    payload.recovery_actions,
    validateRecoveryAction,
  );
  const qualification = validateQualification(payload.qualification);

  if (
    jobCounts === null ||
    openIncidents === null ||
    runtimeWatchdog === null ||
    soakEvidence === undefined ||
    cloudUsage === null ||
    runtimeController === null ||
    activeTasks === null ||
    runtimeIncidents === null ||
    recoveryActions === null ||
    qualification === null
  ) {
    return unavailable();
  }

  return {
    status: "available",
    job_counts: jobCounts,
    open_incidents: openIncidents,
    runtime_watchdog: runtimeWatchdog,
    soak_evidence: soakEvidence,
    cloud_usage: cloudUsage,
    runtime_controller: runtimeController,
    active_tasks: activeTasks,
    runtime_incidents: runtimeIncidents,
    recovery_actions: recoveryActions,
    qualification,
  };
}

export async function readControlPlane(): Promise<ControlPlaneRead> {
  try {
    const response = await fetch(`${CONTROL_PLANE_BASE_URL}/perception/control-plane`, {
      cache: "no-store",
    });
    const payload: unknown = await response.json();
    if (!response.ok) return unavailable();
    return decodeControlPlaneRead(payload);
  } catch {
    return unavailable();
  }
}
