const BASE_URL = process.env.POLYARB_CONTROL_API_URL ?? "https://polyarb-control-api.fly.dev";
const BUSINESS_OVERVIEW_READ_ATTEMPTS = 2;

export type ProductStatus = "available" | "lagging" | "not-published" | "stale" | "unavailable";

export type Product = {
  status: ProductStatus;
  reason_code?: string;
  generation_key?: string;
  parent_structure_generation_key?: string;
  quote_generation_key?: string;
  record_count?: number;
  indexed_record_count?: number;
  count?: number;
  component_counts?: Record<string, number>;
};

export type BusinessOverview = Record<string, unknown> & {
  schema_version: "m1.business-overview.v1";
  status: "available";
  observed_at: string;
  eligibility: { state: "ready" | "paused"; reason_code: string | null };
  structure: Product;
  quote: Product;
  analysis: Product;
  opportunities: Product;
  blockers: Array<{ scope: string; code: string; impact: string }>;
};

export type BusinessOverviewRead =
  | { status: "unavailable"; reason: string }
  | { status: "available"; data: BusinessOverview };

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

const productStatuses = new Set<ProductStatus>(["available", "lagging", "not-published", "stale", "unavailable"]);

function isIsoTimestamp(value: unknown): value is string {
  return typeof value === "string" && !Number.isNaN(Date.parse(value));
}

function isNonNegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function validProduct(value: unknown): value is Product {
  if (!record(value) || typeof value.status !== "string" || !productStatuses.has(value.status as ProductStatus)) return false;
  for (const key of ["reason_code", "generation_key", "parent_structure_generation_key", "quote_generation_key"] as const) {
    if (value[key] !== undefined && typeof value[key] !== "string") return false;
  }
  for (const key of ["record_count", "indexed_record_count", "count"] as const) if (value[key] !== undefined && !isNonNegativeInteger(value[key])) return false;
  if (value.component_counts !== undefined && (!record(value.component_counts) || !Object.values(value.component_counts).every(isNonNegativeInteger))) return false;
  return true;
}

export function decodeBusinessOverview(value: unknown): BusinessOverview | null {
  if (!record(value) || value.schema_version !== "m1.business-overview.v1" || value.status !== "available" || !isIsoTimestamp(value.observed_at) || !record(value.eligibility) || !validProduct(value.structure) || !validProduct(value.quote) || !validProduct(value.analysis) || !validProduct(value.opportunities) || !Array.isArray(value.blockers)) return null;
  if (!(value.eligibility.state === "ready" || value.eligibility.state === "paused") || !(typeof value.eligibility.reason_code === "string" || value.eligibility.reason_code === null)) return null;
  if (!value.blockers.every((item) => record(item) && typeof item.scope === "string" && typeof item.code === "string" && typeof item.impact === "string")) return null;
  return value as BusinessOverview;
}

export async function readBusinessOverview(): Promise<BusinessOverviewRead> {
  for (let attempt = 0; attempt < BUSINESS_OVERVIEW_READ_ATTEMPTS; attempt += 1) {
    try {
      const response = await fetch(`${BASE_URL}/perception/business-overview`, { cache: "no-store" });
      const body: unknown = await response.json();
      const data = decodeBusinessOverview(body);
      if (response.ok && data) return { status: "available", data };
    } catch { /* one bounded retry handles transient cross-cloud reads */ }
  }
  return { status: "unavailable", reason: "business-overview-unavailable" };
}
