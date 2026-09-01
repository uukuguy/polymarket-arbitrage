const BASE_URL = process.env.POLYARB_CONTROL_API_URL ?? "https://polyarb-control-api.fly.dev";

export type ResearchProduct = "structure" | "quotes";
export type ResearchItem = Record<string, unknown> & { entity_id?: string; token_id?: string };
export type ResearchPage = {
  schema_version: "m1.business-research-page.v1";
  product: "structure" | "quote";
  status: "available" | "not-published" | "unavailable";
  generation_key?: string;
  reason_code?: string;
  source_record_count?: number;
  indexed_record_count?: number;
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

export function decodeBusinessResearchPage(value: unknown, product: ResearchProduct): ResearchPage | null {
  const limit = record(value) ? value.limit : undefined;
  if (!record(value) || value.schema_version !== "m1.business-research-page.v1" || !Array.isArray(value.items) || typeof limit !== "number" || !Number.isSafeInteger(limit) || limit < 1 || limit > 200 || !(value.next_after === null || typeof value.next_after === "string")) return null;
  const expected = product === "structure" ? "structure" : "quote";
  if (value.product !== expected || !(value.status === "available" || value.status === "not-published" || value.status === "unavailable")) return null;
  if (value.generation_key !== undefined && typeof value.generation_key !== "string") return null;
  if (value.reason_code !== undefined && typeof value.reason_code !== "string") return null;
  for (const key of ["source_record_count", "indexed_record_count"] as const) {
    if (value[key] !== undefined && !nonNegativeInteger(value[key])) return null;
  }
  const identity = product === "structure" ? "entity_id" : "token_id";
  if (!value.items.every((item) => record(item) && typeof item[identity] === "string")) return null;
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
