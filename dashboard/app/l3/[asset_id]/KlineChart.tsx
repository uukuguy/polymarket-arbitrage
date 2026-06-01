// KlineChart — client component, lightweight-charts v5 candlestick.
//
// CRITICAL CONTRACTS (Plan 05-05 plan-checker iter 2 + Warning #14):
//  1. "use client" directive at top — this file is the client island.
//  2. `lightweight-charts` is imported via `await import(...)` INSIDE useEffect,
//     NOT at the top of the file. The lib calls `document` / `window` /
//     `ResizeObserver` on module load — a top-level static import would crash
//     Next.js 15 RSC during `pnpm build` with "window is not defined".
//  3. v5 series API: `chart.addSeries(CandlestickSeries, options)` —
//     NOT the v4 `chart.addCandlestickSeries(options)` (removed in v5).
//  4. Time is Unix seconds (Math.floor(ms / 1000)) — lightweight-charts
//     refuses ISO strings and millisecond epochs without explicit conversion.
//  5. Cleanup: ResizeObserver.disconnect() + chart.remove() in useEffect
//     return — without these, navigation between L3 pages leaks DOM nodes.
"use client";

import { useEffect, useRef } from "react";
import type { L2OhlcRow } from "@/lib/supabase/l2-queries";

export default function KlineChart({ ohlc }: { ohlc: L2OhlcRow[] }) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    // Local mutable refs so the cleanup closure can reach them after the
    // async IIFE settles.
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let chart: any = null;
    let resizeObserver: ResizeObserver | null = null;
    let cancelled = false;

    (async () => {
      // Dynamic import — lightweight-charts is SSR-unsafe (uses `window` /
      // `document` at module-evaluation time). Keep this INSIDE useEffect.
      const { createChart, CandlestickSeries } = await import(
        "lightweight-charts"
      );
      if (cancelled || !containerRef.current) return;

      chart = createChart(containerRef.current, {
        width: containerRef.current.clientWidth,
        height: 400,
        layout: {
          background: { color: "#0a0a0a" },
          textColor: "#888",
        },
        grid: {
          vertLines: { color: "#1a1a1a" },
          horzLines: { color: "#1a1a1a" },
        },
        timeScale: { timeVisible: true, secondsVisible: false },
      });

      const series = chart.addSeries(CandlestickSeries, {
        upColor: "#26a69a",
        downColor: "#ef5350",
        wickUpColor: "#26a69a",
        wickDownColor: "#ef5350",
        borderVisible: false,
      });

      series.setData(
        ohlc.map((r) => ({
          time: Math.floor(new Date(r.bucket_ts).getTime() / 1000),
          open: Number(r.open),
          high: Number(r.high),
          low: Number(r.low),
          close: Number(r.close),
        })),
      );

      // Responsive width — fits parent grid column.
      resizeObserver = new ResizeObserver((entries) => {
        const w = entries[0]?.contentRect.width;
        if (w && chart) chart.applyOptions({ width: w });
      });
      resizeObserver.observe(containerRef.current);
    })();

    return () => {
      cancelled = true;
      resizeObserver?.disconnect();
      chart?.remove();
    };
  }, [ohlc]);

  return (
    <div
      ref={containerRef}
      style={{ width: "100%", height: 400, background: "#0a0a0a" }}
    />
  );
}
