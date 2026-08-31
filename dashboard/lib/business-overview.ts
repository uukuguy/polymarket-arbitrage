const BASE_URL = process.env.POLYARB_CONTROL_API_URL ?? "https://polyarb-control-api.fly.dev";

export type BusinessOverview = Record<string, unknown> & {
  schema_version: "m1.business-overview.v1";
  status: "available";
  observed_at: string;
  eligibility: { state: string; reason_code: string | null };
  structure: { status: string; generation_key?: string };
  quote: { status: string; generation_key?: string; parent_structure_generation_key?: string };
  analysis: { status: string };
  opportunities: { status: string; count?: number; quote_generation_key?: string };
  blockers: Array<{ scope: string; code: string; impact: string }>;
};

export type BusinessOverviewRead =
  | { status: "unavailable"; reason: string }
  | { status: "available"; data: BusinessOverview };

function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function decodeBusinessOverview(value: unknown): BusinessOverview | null {
  if (!record(value) || value.schema_version !== "m1.business-overview.v1" || value.status !== "available" || typeof value.observed_at !== "string" || !record(value.eligibility) || !record(value.structure) || !record(value.quote) || !record(value.analysis) || !record(value.opportunities) || !Array.isArray(value.blockers)) return null;
  if (typeof value.eligibility.state !== "string" || !(typeof value.eligibility.reason_code === "string" || value.eligibility.reason_code === null)) return null;
  for (const product of [value.structure, value.quote, value.analysis, value.opportunities]) if (typeof product.status !== "string") return null;
  const count = value.opportunities.count;
  if (count !== undefined && (typeof count !== "number" || !Number.isSafeInteger(count) || count < 0)) return null;
  if (!value.blockers.every((item) => record(item) && typeof item.scope === "string" && typeof item.code === "string" && typeof item.impact === "string")) return null;
  return value as BusinessOverview;
}

export async function readBusinessOverview(): Promise<BusinessOverviewRead> {
  try {
    const response = await fetch(`${BASE_URL}/perception/business-overview`, { cache: "no-store" });
    const body: unknown = await response.json();
    const data = decodeBusinessOverview(body);
    return response.ok && data ? { status: "available", data } : { status: "unavailable", reason: "business-overview-unavailable" };
  } catch { return { status: "unavailable", reason: "business-overview-unavailable" }; }
}
