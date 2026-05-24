"""Polymarket Data API client — /trades 7-day historical backfill (D-08).

Phase 03 Plan 06 Task 6.

Open Q 2 RESOLVED (Plan 06 Task 0 probe, 2026-05-24): the Polymarket Data API
/trades endpoint does NOT support server-side filtering by time, asset, or
event. Tested `beforeTimestamp` / `before` / `maxTimestamp` / `endTimestamp`
/ `asset=<token>` / `eventSlug=<slug>` — all silently ignored (returned the
latest-trades feed regardless). Only `user=<wallet>` and `takerOnly=true`
appear to filter server-side.

Strategy: paginate the GLOBAL trades feed via offset (cap MAX_OFFSET=1000;
live probe shows 3000 OK but 4000 → HTTP 400), client-side filter rows whose
`asset` matches the target asset_id, and break iteration when a row with
`timestamp < cutoff_ts` is observed.

Practical implication for backfill coverage:
- Heavily-traded assets: the recent 1500 global rows will contain enough
  per-asset rows to meaningfully seed l2_trades.
- Thinly-traded assets: per-asset coverage during 7-day backfill is
  best-effort. M3+ may add a proper Polymarket subgraph-backed historical
  source if l2_trades sparsity hurts backtest fidelity.

Pattern (Phase 02 GammaClient verbatim analog):
- Long-lived httpx.AsyncClient with HTTP/2 + keepalive
- aiolimiter.AsyncLimiter(150, 10) — 25% headroom under 200/10s published rate
- tenacity AsyncRetrying with stop_after_attempt + wait_exponential
- 429 path: log warning + asyncio.sleep(10) + re-raise so tenacity retries
- follow_redirects=False (Phase 02 F-2 SECURITY)
- T-03-06-04 defensive filter: drop rows with size <= 0
- T-03-06-06: never log response body on 4xx/5xx (only status_code)

Trade dict shape (per probe):
- asset: str (token id, 64-char numeric)
- proxyWallet: str (0x-prefixed)
- side: 'BUY' | 'SELL'
- size: float (USDC)
- price: float (0..1)
- timestamp: int (unix seconds)
- transactionHash: str (0x-prefixed; UNIQUE per blockchain tx)
- conditionId, slug, eventSlug, outcome, outcomeIndex, title, icon, name,
  pseudonym, bio, profileImage, profileImageOptimized (display-only)

CLI:
    python -m polyarb.clients.data_api_client --market <asset_id> --days 7
"""
from __future__ import annotations

import argparse
import asyncio
import time
from typing import AsyncIterator

import httpx
from aiolimiter import AsyncLimiter
from loguru import logger
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

# ── Module constants (locked per RESEARCH Focus 6) ─────────────────────────

DATA_API_BASE = "https://data-api.polymarket.com"

# Live-probe verified ceiling: offset=3000 → 200, offset=4000 → 400.
# Conservative cap of 1000 keeps us well clear of the cliff edge AND
# matches RESEARCH Focus 6's recommended budget.
MAX_OFFSET: int = 1000

# Polymarket Data API published rate: ~200 req/10s. We dial 150/10s
# (25% headroom) so transient bursts don't trip 429.
RATE_PER_10S: int = 150
PAGE_SIZE: int = 500

# Module-level limiter — shared across all backfill invocations in the same
# process so per-asset concurrency from candidate_refresh respects the budget.
# Kept eagerly initialized for module-level introspection (tests assert
# RATE_PER_10S=150 and time_period=10). aiolimiter warns when reused across
# event loops; production daemon uses a single loop so this is benign. Tests
# that spin separate loops via pytest-asyncio see the warning but no
# behavioral impact at our request volumes.
_LIMITER = AsyncLimiter(RATE_PER_10S, 10)

# 429 backoff — `asyncio.sleep(_SLEEP_ON_429)` before re-raising so tenacity
# retries with the rate-limit headroom restored.
_SLEEP_ON_429: int = 10


# ── Public API ─────────────────────────────────────────────────────────────


async def backfill_trades_for_asset(
    asset_id: str,
    *,
    days: int = 7,
    page_size: int = PAGE_SIZE,
    client: httpx.AsyncClient | None = None,
) -> AsyncIterator[dict]:
    """Yield up to `days` of trades for `asset_id` from the Polymarket Data API.

    Iteration ends when ANY of:
    - A trade with `timestamp < now - days*86400` is observed (cutoff reached)
    - All MAX_OFFSET pages exhausted with no further rows
    - HTTP 400 (offset cliff exceeded) — logged, iteration stops cleanly

    Client-side dedup: a trade with a transactionHash already yielded in this
    run is skipped (defensive; should not normally occur with offset
    pagination but cheap to enforce).

    Defensive filter: drops rows with `size <= 0` (T-03-06-04 — poisoned
    payload protection).

    Args:
        asset_id: Polymarket asset (token) id to filter trades by. Filtering
            is client-side; the Data API does NOT support server-side asset
            filtering.
        days: Cutoff window — only yield trades within the last `days` days.
        page_size: Per-request limit. Caps at 500 (Data API rejects larger).
        client: Optional pre-built httpx.AsyncClient. When None, this function
            owns a transient client and closes it on exit. For batched
            multi-asset backfill, callers SHOULD pre-build and share a
            client to amortize the TLS + HTTP/2 handshake.
    """
    cutoff_ts = int(time.time()) - days * 86400
    seen_hashes: set[str] = set()
    owns_client = client is None
    if client is None:
        client = httpx.AsyncClient(
            timeout=15.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            headers={"User-Agent": "polyarb/0.1"},
            http2=True,
            follow_redirects=False,  # Phase 02 F-2 SECURITY
        )

    try:
        cutoff_reached = False
        for offset in range(0, MAX_OFFSET + 1, page_size):
            try:
                page = await _fetch_page(client, params={"limit": page_size, "offset": offset})
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 400:
                    # Offset cliff hit (probe: 4000+ → 400). Log and stop cleanly.
                    logger.warning(
                        f"data-api: offset={offset} returned 400 (cliff); "
                        f"stopping iteration after yielding so far"
                    )
                    break
                raise

            if not isinstance(page, list):
                logger.warning(f"data-api: unexpected response shape {type(page).__name__}")
                break

            page_yielded = 0
            for trade in page:
                if not isinstance(trade, dict):
                    continue
                # Asset filter (Open Q 2: server-side filter unavailable)
                if trade.get("asset") != asset_id:
                    continue
                # Cutoff check (timestamps decrease with offset → break on first old row)
                ts = trade.get("timestamp")
                if isinstance(ts, (int, float)) and ts < cutoff_ts:
                    cutoff_reached = True
                    break
                # Defensive filter (T-03-06-04)
                size = trade.get("size", 0)
                if not isinstance(size, (int, float)) or size <= 0:
                    continue
                # Dedup
                tx_hash = trade.get("transactionHash")
                if not tx_hash or tx_hash in seen_hashes:
                    continue
                seen_hashes.add(tx_hash)
                yield trade
                page_yielded += 1

            if cutoff_reached:
                break

            # Short page → no more data globally
            if len(page) < page_size:
                break

        # If we exhausted MAX_OFFSET without hitting cutoff and the last page was
        # full, the global feed has more history but Data API offset cap blocks us.
        # M3+ subgraph backfill is the proper escape hatch (documented above).
    finally:
        if owns_client:
            await client.aclose()


async def _fetch_page(client: httpx.AsyncClient, *, params: dict) -> list[dict]:
    """One paginated GET with rate limiter + tenacity retry + 429 backoff.

    Retries on httpx.RequestError / httpx.TimeoutException / 5xx / 429 with
    exponential backoff. 4xx (other than 429) and JSONDecodeError propagate.
    """
    url = f"{DATA_API_BASE}/trades"
    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=8),
        retry=retry_if_exception_type(
            (httpx.RequestError, httpx.HTTPStatusError, httpx.TimeoutException)
        ),
        reraise=True,
    ):
        with attempt:
            async with _LIMITER:
                resp = await client.get(url, params=params)
                if resp.status_code == 429:
                    logger.warning(
                        f"data-api: 429 rate-limited, sleeping {_SLEEP_ON_429}s "
                        f"then retrying (params={params})"
                    )
                    await asyncio.sleep(_SLEEP_ON_429)
                    # Re-raise so tenacity retries with the rate-limit headroom.
                    resp.raise_for_status()
                # Non-retryable 4xx (excluding 429) — propagate without retry
                if 400 <= resp.status_code < 500 and resp.status_code != 429:
                    # Log status only — T-03-06-06 never log response body
                    logger.warning(
                        f"data-api: non-retryable {resp.status_code} for params={params}"
                    )
                    resp.raise_for_status()
                resp.raise_for_status()
                return resp.json()
    # Unreachable — AsyncRetrying with reraise=True raises before falling through
    raise RuntimeError("data-api: AsyncRetrying exited without yielding")


# ── CLI entrypoint (make backfill-trades MARKET=<asset_id>) ────────────────


def _cli() -> int:
    parser = argparse.ArgumentParser(
        prog="polyarb-backfill-trades",
        description="Backfill 7 days of Polymarket /trades for one asset (D-08).",
    )
    parser.add_argument(
        "--market",
        required=True,
        help="Asset (token) id — Polymarket /trades 'asset' field",
    )
    parser.add_argument("--days", type=int, default=7, help="cutoff window in days")
    parser.add_argument(
        "--limit", type=int, default=PAGE_SIZE, help=f"per-page size (default {PAGE_SIZE})"
    )
    args = parser.parse_args()

    async def _run() -> int:
        n = 0
        async for trade in backfill_trades_for_asset(
            asset_id=args.market, days=args.days, page_size=args.limit
        ):
            n += 1
            # Print one trade per line (json) — operators pipe to jq / wc -l
            import json

            print(json.dumps(trade, ensure_ascii=False))
        logger.info(f"backfill complete: asset_id={args.market} yielded={n}")
        return 0

    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(_cli())
