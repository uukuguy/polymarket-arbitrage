#!/usr/bin/env python
"""smoke_l2_ws.py — 30s WS sanity against a known liquid Polymarket asset.

Phase 03 Plan 04 (D-02) manual smoke. Connects to Polymarket WS market
channel, subscribes to a hardcoded liquid asset_id for ``duration_s``
seconds, prints frame counts by event_type, then exits.

Usage:
    uv run python scripts/smoke_l2_ws.py
    # or with a custom asset:
    uv run python scripts/smoke_l2_ws.py <current-asset-id>

Expected output (live asset):
    === Smoke result (30s) ===
      price_change: 17
      best_bid_ask: 4
      book: 1
      TOTAL: 22

Exit codes:
    0 — frames received, schema looks live
    1 — all frames were unknown event_type (schema may have shifted)
    2 — zero frames received in window (asset_id stale or WS unreachable)
"""
from __future__ import annotations

import asyncio
import sys
from collections import Counter

from polyarb.clients.ws_market_client import stream_market_events
from polyarb.observability.logging import init_logging

# A currently-liquid market token id (2026-05-24: "Iraq 2026 World Cup",
# liquidity ~$10M; smoke validated 3 frames in 30s incl. initial book).
# If the smoke prints zero frames, replace with a current asset_id:
#   curl -s 'https://gamma-api.polymarket.com/markets?active=true&closed=false' \
#     --get --data 'order=liquidityNum' --data 'ascending=false' --data 'limit=1' \
#     | python3 -c 'import sys,json; m=json.load(sys.stdin)[0]; \
#                    t=json.loads(m["clobTokenIds"]); print(t[0])'
DEFAULT_ASSET = "53465512181802150755993130711224070738002100921790051090044528012833736167995"


async def smoke(asset_id: str, duration_s: int = 30) -> int:
    init_logging()
    counts: Counter[str] = Counter()

    async def _consume() -> None:
        async for event in stream_market_events([asset_id], initial_dump=True):
            event_type = event.get("event_type", "unknown")
            counts[event_type] += 1

    try:
        await asyncio.wait_for(_consume(), timeout=duration_s)
    except TimeoutError:
        # Expected — we want exactly duration_s of listening
        pass
    except asyncio.CancelledError:
        # Allow Ctrl-C to exit cleanly
        pass

    total = sum(counts.values())
    print(f"=== Smoke result ({duration_s}s) ===")
    for t, n in counts.most_common():
        print(f"  {t}: {n}")
    print(f"  TOTAL: {total}")

    if total == 0:
        print("FAIL: zero frames in window — asset_id may be stale or WS unreachable")
        return 2
    if counts.get("unknown", 0) == total:
        print("WARN: all frames were unknown event_type — schema may have shifted")
        return 1
    return 0


if __name__ == "__main__":
    asset = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_ASSET
    sys.exit(asyncio.run(smoke(asset)))
