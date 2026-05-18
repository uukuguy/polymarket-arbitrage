// /scan — recipe trigger button (D-21).
// Protected by middleware (auth + email whitelist) before this component renders.
"use client";

import { useState } from "react";
import { runScan } from "@/lib/scan-client";
import type { ScanResponse } from "@/lib/types";

const RECIPES = [
  "thick-but-slippery",
  "near-end",
  "ghost-suspicious",
  "coin-flip",
  "neg-risk-incomplete",
  "scan-by-tag",
];

export default function ScanPage() {
  const [recipe, setRecipe] = useState(RECIPES[0]);
  const [limit, setLimit] = useState(50);
  const [result, setResult] = useState<ScanResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [startedAt, setStartedAt] = useState<number | null>(null);

  async function handleRun() {
    setLoading(true);
    setResult(null);
    setStartedAt(Date.now());
    try {
      const r = await runScan({ recipe_name: recipe, params: { limit } });
      setResult(r);
    } catch (e) {
      setResult({ error: e instanceof Error ? e.message : "unknown error" });
    } finally {
      setLoading(false);
    }
  }

  const elapsedMs = startedAt ? Date.now() - startedAt : null;

  return (
    <main style={{ padding: 24 }}>
      <h1 style={{ fontSize: 24, marginBottom: 8 }}>Run scan recipe</h1>
      <p style={{ fontSize: 13, color: "#888", marginBottom: 16 }}>
        Posts to <code>/api/scan</code> (Vercel Edge) → HMAC-SHA256 sign →
        Fly <code>/scan</code>. P1 trust-split honored at daemon side.
      </p>
      <div style={{ display: "flex", gap: 12, alignItems: "center", marginBottom: 16 }}>
        <label style={{ fontSize: 13 }}>
          Recipe:&nbsp;
          <select
            value={recipe}
            onChange={(e) => setRecipe(e.target.value)}
            style={{
              padding: 6,
              background: "#111",
              color: "#e5e5e5",
              border: "1px solid #333",
              borderRadius: 4,
            }}
          >
            {RECIPES.map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </label>
        <label style={{ fontSize: 13 }}>
          Limit:&nbsp;
          <input
            type="number"
            min={1}
            max={500}
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value))}
            style={{
              padding: 6,
              width: 70,
              background: "#111",
              color: "#e5e5e5",
              border: "1px solid #333",
              borderRadius: 4,
            }}
          />
        </label>
        <button
          onClick={handleRun}
          disabled={loading}
          style={{
            padding: "8px 16px",
            background: loading ? "#444" : "#1d4ed8",
            color: "white",
            border: 0,
            borderRadius: 4,
            cursor: loading ? "not-allowed" : "pointer",
          }}
        >
          {loading ? "running..." : "Run"}
        </button>
      </div>
      {loading && (
        <p style={{ fontSize: 13, color: "#9ec5fe" }}>
          Forwarding to Fly daemon (typical 2-10s)...
        </p>
      )}
      {result && (
        <section style={{ marginTop: 12 }}>
          {result.error ? (
            <div
              style={{
                background: "#3b0a0a",
                border: "1px solid #6b1010",
                padding: 12,
                borderRadius: 4,
                fontSize: 13,
                color: "#ffb0b0",
              }}
            >
              Error: {result.error}
            </div>
          ) : (
            <p style={{ fontSize: 13, color: "#888" }}>
              {result.row_count ?? 0} rows
              {elapsedMs !== null ? ` · ${elapsedMs}ms` : ""}
            </p>
          )}
          <pre
            style={{
              marginTop: 8,
              padding: 12,
              background: "#0d0d0d",
              border: "1px solid #222",
              borderRadius: 4,
              fontSize: 12,
              overflow: "auto",
              maxHeight: 480,
            }}
          >
            {JSON.stringify(result, null, 2)}
          </pre>
        </section>
      )}
    </main>
  );
}
