// Typed wrapper around /api/scan — keeps the client page free of fetch boilerplate.
import type { ScanRequestBody, ScanResponse } from "./types";

export async function runScan(body: ScanRequestBody): Promise<ScanResponse> {
  const res = await fetch("/api/scan", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  let parsed: ScanResponse;
  try {
    parsed = (await res.json()) as ScanResponse;
  } catch {
    parsed = { error: `Non-JSON response (status ${res.status})` };
  }
  if (!res.ok && !parsed.error) {
    parsed.error = `HTTP ${res.status}`;
  }
  return parsed;
}
