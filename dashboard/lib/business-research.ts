const BASE_URL = process.env.POLYARB_CONTROL_API_URL ?? "https://polyarb-control-api.fly.dev";

export type ResearchProduct = "structure" | "quotes" | "analysis";
export type ResearchItem = Record<string, unknown> & { entity_id?: string; token_id?: string; group_id?: string };
export type ResearchPage = {
  schema_version: "m1.business-research-page.v1";
  product: "structure" | "quote" | "analysis";
  status: "available" | "not-published" | "unavailable";
  generation_key?: string;
  reason_code?: string;
  source_record_count?: number;
  indexed_record_count?: number;
  summary?: Record<string, unknown>;
  items: ResearchItem[];
  limit: number;
  next_after: string | null;
};

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

const QUOTE_DISCOVERY_REASONS = new Set([
  "meaningful-executable-depth",
  "non-neutral-yes-price",
  "insufficient-executable-depth",
  "missing-or-invalid-quote",
  "not-executable",
]);

function finiteNonNegative(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value) && value >= 0;
}

function validQuoteContext(value: unknown): boolean {
  if (!record(value) || typeof value.status !== "string") return false;
  if (value.status === "not-indexed") return Object.keys(value).length === 1;
  return value.status === "available";
}

function validQuoteDiscoveryItem(value: Record<string, unknown>): boolean {
  const discovery = value.discovery;
  return record(discovery)
    && finiteNonNegative(discovery.executable_notional_usd)
    && finiteNonNegative(discovery.price_extremity_bps)
    && finiteNonNegative(discovery.score)
    && Array.isArray(discovery.reasons)
    && discovery.reasons.every((reason) => typeof reason === "string" && QUOTE_DISCOVERY_REASONS.has(reason))
    && validQuoteContext(value.event_context)
    && validQuoteContext(value.neg_risk_context);
}

export function decodeBusinessResearchPage(value: unknown, product: ResearchProduct): ResearchPage | null {
  const limit = record(value) ? value.limit : undefined;
  if (!record(value) || value.schema_version !== "m1.business-research-page.v1" || !Array.isArray(value.items) || typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > 200 || !(value.next_after === null || typeof value.next_after === "string")) return null;
  const expected = product === "structure" ? "structure" : product === "quotes" ? "quote" : "analysis";
  if (value.product !== expected || !(value.status === "available" || value.status === "not-published" || value.status === "unavailable")) return null;
  if (value.generation_key !== undefined && typeof value.generation_key !== "string") return null;
  if (value.reason_code !== undefined && typeof value.reason_code !== "string") return null;
  if (value.summary !== undefined && !record(value.summary)) return null;
  for (const key of ["source_record_count", "indexed_record_count"] as const) {
    if (value[key] !== undefined && !nonNegativeInteger(value[key])) return null;
  }
  const identity = product === "structure" ? "entity_id" : product === "quotes" ? "token_id" : "group_id";
  if (!value.items.every((item) => record(item) && typeof item[identity] === "string")) return null;
  if (product === "quotes" && !value.items.every((item) => validQuoteDiscoveryItem(item as Record<string, unknown>))) return null;
  return value as ResearchPage;
}

export async function readBusinessResearchPage(product: ResearchProduct, after = ""): Promise<ResearchPage | null> {
  try {
    const params = new URLSearchParams({ limit: "100" });
    if (after) params.set("after", after);
    const response = await fetch(`${BASE_URL}/perception/business/${product}?${params}`, { cache: "no-store" });
    const page = decodeBusinessResearchPage(await response.json(), product);
    return response.ok && page ? page : null;
  } catch { return null; }
}

export type QuoteCoverageItem = {
  group_id: string;
  event_id?: string;
  coverage_state: "coverage-gap" | "analysis-ready" | "healthy" | "needs-context";
  candidate_state: string;
  expected_member_count: number;
  quoted_member_count: number;
  missing_member_count: number;
  quality?: string;
  event: Record<string, unknown>;
  action: string;
};
export type QuoteCoveragePage = {
  schema_version: "m1.quote-coverage-page.v1";
  status: "available" | "not-published" | "unavailable";
  generation_key?: string;
  parent_structure_generation_key?: string;
  reason_code?: string;
  summary?: Record<string, unknown>;
  items: QuoteCoverageItem[];
  limit: number;
  next_after: string | null;
};

export function decodeQuoteCoveragePage(value: unknown): QuoteCoveragePage | null {
  if (!record(value) || value.schema_version !== "m1.quote-coverage-page.v1"
    || !(value.status === "available" || value.status === "not-published" || value.status === "unavailable")
    || !Array.isArray(value.items) || !nonNegativeInteger(value.limit) || value.limit < 1 || value.limit > 200
    || !(value.next_after === null || typeof value.next_after === "string")) return null;
  if (value.generation_key !== undefined && typeof value.generation_key !== "string") return null;
  if (value.parent_structure_generation_key !== undefined && typeof value.parent_structure_generation_key !== "string") return null;
  if (value.reason_code !== undefined && typeof value.reason_code !== "string") return null;
  if (value.summary !== undefined && !record(value.summary)) return null;
  const states = new Set(["coverage-gap", "analysis-ready", "healthy", "needs-context"]);
  if (!value.items.every((item) => record(item)
    && typeof item.group_id === "string" && states.has(String(item.coverage_state))
    && typeof item.candidate_state === "string" && nonNegativeInteger(item.expected_member_count)
    && nonNegativeInteger(item.quoted_member_count) && nonNegativeInteger(item.missing_member_count)
    && record(item.event) && typeof item.action === "string")) return null;
  return value as QuoteCoveragePage;
}

export async function readQuoteCoveragePage(): Promise<QuoteCoveragePage | null> {
  try {
    const response = await fetch(`${BASE_URL}/perception/business/quote-coverage?limit=100`, { cache: "no-store" });
    const page = decodeQuoteCoveragePage(await response.json());
    return response.ok && page ? page : null;
  } catch { return null; }
}

export type EventResearchDetail = Record<string, unknown> & {
  schema_version: "m1.event-research-detail.v1";
  status: "available" | "unavailable" | "not-published";
  reason_code?: string;
  event_id: string;
  event?: Record<string, unknown>;
  anchor?: Record<string, unknown>;
  state_counts?: Record<string, number>;
  groups: EventResearchGroup[];
  focused_group?: EventResearchGroup | null;
  cautions?: string[];
};
export type EventResearchGroup = Record<string, unknown> & {
  group_id: string;
  structure: Record<string, unknown>;
  candidate_state: string;
  candidate: Record<string, unknown>;
  quote_coverage: Record<string, unknown>;
};
export type EventResearchDetailOptions = { focusGroupId?: string; observedGeneration?: string };

const EVENT_COVERAGE_STATES = new Set(["unknown", "incomplete-missing", "complete-non-executable", "complete-executable"]);

function validEventResearchAnchor(value: unknown): boolean {
  return record(value)
    && typeof value.quote_generation_key === "string"
    && typeof value.structure_generation_key === "string"
    && typeof value.changed_since_entry === "boolean"
    && typeof value.materialized_at === "string"
    && !Number.isNaN(Date.parse(value.materialized_at));
}

function validEventResearchGroup(value: unknown): value is EventResearchGroup {
  if (!record(value) || typeof value.group_id !== "string" || !value.group_id || !record(value.structure)
    || typeof value.candidate_state !== "string" || !record(value.candidate) || !record(value.quote_coverage)) return false;
  const coverage = value.quote_coverage;
  if (!EVENT_COVERAGE_STATES.has(String(coverage.coverage_state))) return false;
  for (const key of ["observed", "executable", "non_executable"] as const) if (!nonNegativeInteger(coverage[key])) return false;
  if (!(coverage.expected === null || nonNegativeInteger(coverage.expected)) || !(coverage.missing === null || nonNegativeInteger(coverage.missing))) return false;
  // These are the only nullable fields emitted by the Structure group source.
  // Null means the source could not establish the fact; it must not be made up
  // as zero by the dashboard decoder.
  for (const key of ["expected_member_count", "active_named_count"] as const) {
    if (value.structure[key] !== undefined && value.structure[key] !== null && !nonNegativeInteger(value.structure[key])) return false;
  }
  for (const key of ["event_id", "neg_risk_type", "quality", "reason"] as const) {
    if (value.structure[key] !== undefined && value.structure[key] !== null && typeof value.structure[key] !== "string") return false;
  }
  for (const key of ["market_count", "active_market_count"] as const) {
    if (value.structure[key] !== undefined && !nonNegativeInteger(value.structure[key])) return false;
  }
  for (const key of ["bundle_cost", "max_bundle_size", "capital_required_usd", "gross_profit_usd", "gross_roi_bps"] as const) {
    if (value.candidate[key] !== undefined && !finiteNonNegative(value.candidate[key])) return false;
  }
  return true;
}

export function decodeEventResearchDetail(value: unknown, focusGroupId?: string): EventResearchDetail | null {
  if (!record(value) || value.schema_version !== "m1.event-research-detail.v1"
    || !(value.status === "available" || value.status === "unavailable" || value.status === "not-published")
    || typeof value.event_id !== "string" || !value.event_id || !Array.isArray(value.groups)
    || (value.reason_code !== undefined && typeof value.reason_code !== "string")) return null;
  if (value.status !== "available") {
    if (value.groups.length !== 0 || value.event !== undefined || value.anchor !== undefined || value.focused_group !== undefined
      || value.state_counts !== undefined || value.structure !== undefined || value.quote_coverage !== undefined || value.analysis !== undefined) return null;
    return value as EventResearchDetail;
  }
  if (!record(value.event) || !validEventResearchAnchor(value.anchor) || !record(value.state_counts)
    || !record(value.structure) || !record(value.quote_coverage) || !record(value.analysis)
    || !Array.isArray(value.cautions) || !value.cautions.every((caution) => typeof caution === "string")
    || !value.groups.every(validEventResearchGroup)) return null;
  if (!Object.values(value.state_counts).every(nonNegativeInteger)) return null;
  for (const key of ["liquidity", "volume"] as const) if (value.event[key] !== undefined && value.event[key] !== null && !finiteNonNegative(value.event[key])) return null;
  for (const key of ["market_count", "active_market_count"] as const) if (value.event[key] !== undefined && !nonNegativeInteger(value.event[key])) return null;
  for (const key of ["group_count"] as const) if (!nonNegativeInteger(value.structure[key])) return null;
  for (const key of ["expected", "observed", "executable", "non_executable", "missing"] as const) {
    if (!(value.quote_coverage[key] === null || nonNegativeInteger(value.quote_coverage[key]))) return null;
  }
  if (value.analysis.research_only !== true) return null;
  if (!Array.isArray(value.blockers) || !value.blockers.every((blocker) => record(blocker) && typeof blocker.code === "string" && nonNegativeInteger(blocker.count))) return null;
  if (!(value.focused_group === null || value.focused_group === undefined || validEventResearchGroup(value.focused_group))) return null;
  const focused = value.focused_group;
  if (focused && !value.groups.some((group) => group.group_id === focused.group_id)) return null;
  if (focusGroupId && (!focused || focused.group_id !== focusGroupId)) return null;
  return value as EventResearchDetail;
}

export async function readEventResearchDetail(eventId: string, options: EventResearchDetailOptions = {}): Promise<EventResearchDetail | null> {
  try {
    const params = new URLSearchParams();
    if (options.focusGroupId) params.set("focus_group_id", options.focusGroupId);
    if (options.observedGeneration) params.set("observed_generation", options.observedGeneration);
    const suffix = params.size ? `?${params}` : "";
    const response = await fetch(`${BASE_URL}/perception/business/events/${encodeURIComponent(eventId)}${suffix}`, { cache: "no-store" });
    const detail = decodeEventResearchDetail(await response.json(), options.focusGroupId);
    return response.ok && detail ? detail : null;
  } catch { return null; }
}

export type StructureIntelligenceStatus = "available" | "unavailable";
export type StructureIntelligenceSummary = {
  schema_version: "m1.structure-intelligence.v1";
  status: StructureIntelligenceStatus;
  generation_key?: string;
  reason_code?: string;
  event_count?: number;
  market_count?: number;
  open_event_count?: number;
  detached_group_count?: number;
  projection_octets?: number;
  source_components?: Record<string, number>;
};
export type StructureIntelligenceItem = Record<string, unknown> & { event_id?: string; group_id?: string };
export type StructureIntelligencePage = {
  schema_version: "m1.structure-intelligence.v1";
  status: StructureIntelligenceStatus;
  generation_key?: string;
  reason_code?: string;
  product?: "events" | "groups";
  items: StructureIntelligenceItem[];
  limit: number;
  next_after: string | null;
};

export function decodeStructureIntelligenceSummary(value: unknown): StructureIntelligenceSummary | null {
  if (!record(value) || value.schema_version !== "m1.structure-intelligence.v1" || (value.status !== "available" && value.status !== "unavailable")) return null;
  for (const key of ["event_count", "market_count", "open_event_count", "detached_group_count", "projection_octets"] as const) if (value[key] !== undefined && !nonNegativeInteger(value[key])) return null;
  if (value.generation_key !== undefined && typeof value.generation_key !== "string") return null;
  if (value.reason_code !== undefined && typeof value.reason_code !== "string") return null;
  if (value.source_components !== undefined && (!record(value.source_components) || !Object.values(value.source_components).every(nonNegativeInteger))) return null;
  return value as StructureIntelligenceSummary;
}

export function decodeStructureIntelligencePage(value: unknown, product: "events" | "groups"): StructureIntelligencePage | null {
  if (!record(value) || value.schema_version !== "m1.structure-intelligence.v1" || (value.status !== "available" && value.status !== "unavailable") || !Array.isArray(value.items) || !nonNegativeInteger(value.limit) || value.limit < 1 || value.limit > 200 || !(value.next_after === null || typeof value.next_after === "string")) return null;
  if (value.product !== product || !value.items.every((item) => record(item) && typeof item[product === "events" ? "event_id" : "group_id"] === "string")) return null;
  return value as StructureIntelligencePage;
}

export async function readStructureIntelligenceSummary(): Promise<StructureIntelligenceSummary | null> {
  try {
    const response = await fetch(`${BASE_URL}/perception/business/structure/summary`, { cache: "no-store" });
    const result = decodeStructureIntelligenceSummary(await response.json());
    return response.ok && result ? result : null;
  } catch { return null; }
}

export async function readStructureIntelligencePage(
  product: "events" | "groups",
  options: { openOnly?: boolean } = {},
): Promise<StructureIntelligencePage | null> {
  try {
    const params = new URLSearchParams({ limit: "100" });
    if (product === "events" && options.openOnly === true) params.set("open_only", "true");
    const response = await fetch(`${BASE_URL}/perception/business/structure/${product}?${params}`, { cache: "no-store" });
    const result = decodeStructureIntelligencePage(await response.json(), product);
    return response.ok && result ? result : null;
  } catch { return null; }
}
