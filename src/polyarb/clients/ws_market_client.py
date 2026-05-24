"""Polymarket WS market-channel client — long-lived async iterator.

Plan 03-04 D-02. Drives the L2 daemon's primary data plane:
- Subscribes to a candidate asset_ids list on `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Yields `price_change` / `best_bid_ask` / `last_trade_price` / `book` JSON events
- Uses the websockets 15+ reconnect-iterator pattern (`async for ws in websockets.connect(...)`)
- The transport-level reconnect is handled by the iterator; the application-level
  30s silence watchdog is in `polyarb.daemon.ws_watchdog` (Plan 03-04 D-03).

Critical gotchas (do NOT relax):
1. ``ping_interval=10`` — Polymarket WS server drops the connection at ~10s
   silence. Using the websockets default 20s causes silent disconnects within
   minutes (docs.polymarket.com).
2. ``max_size=2**22`` (4 MiB) — `initial_dump=True` book snapshots can be large.
   Default 2**20 (1 MiB) throws ``PayloadTooBig`` on big orderbooks. Phase 02
   D-23 precedent: fat payloads bite.
3. ``async for ws in websockets.connect(...)`` — the **reconnect-iterator**
   form. NEVER use ``async with`` here — that disables auto-reconnect.
4. ``asyncio.CancelledError`` must propagate (Phase 02 F-04). SIGTERM cannot
   interrupt mid-frame unless CancelledError bubbles out.

T-03-04-01 mitigation: log only frame *type* at DEBUG, never the body. Frame
contents are market data — low-value but unnecessary to leak to logs.
"""
from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator

import websockets
from loguru import logger

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# Polymarket REQUIRES 10s ping (server drops at ~10s silence). NOT the
# websockets default 20s.
PING_INTERVAL_S = 10

# 4 MiB cap for fat initial_dump book snapshots. Default 2**20 (1 MiB) is
# insufficient on large orderbooks. Phase 02 D-23 OOM precedent.
MAX_FRAME_SIZE = 2**22


async def stream_market_events(
    assets_ids: list[str],
    *,
    initial_dump: bool = True,
    ping_interval_s: int = PING_INTERVAL_S,
) -> AsyncIterator[dict]:
    """Long-lived async iterator yielding market-channel events.

    Args:
        assets_ids: list of token ids (canonical id field on Polymarket).
        initial_dump: if True (default), subscribe payload requests a full
            book snapshot on connect/reconnect so the consumer always has a
            baseline after a reconnect (no held-state drift).
        ping_interval_s: ping cadence (default 10s). DO NOT raise above 10
            — Polymarket drops the socket at ~10s silence.

    Yields:
        Parsed JSON event dicts. Malformed frames (JSONDecodeError) are
        logged at warning and skipped — the iterator continues.

    Raises:
        asyncio.CancelledError: propagated unchanged (Phase 02 F-04).
    """
    if not assets_ids:
        logger.warning("ws_market_client: empty assets_ids list — nothing to subscribe")
        return

    async for ws in websockets.connect(
        WS_URL,
        ping_interval=ping_interval_s,
        ping_timeout=ping_interval_s,
        max_size=MAX_FRAME_SIZE,
    ):
        try:
            sub = {
                "type": "market",
                "assets_ids": assets_ids,
                "initial_dump": initial_dump,
            }
            await ws.send(json.dumps(sub))
            logger.info(
                f"ws subscribed: {len(assets_ids)} assets, initial_dump={initial_dump}"
            )
            async for raw in ws:
                try:
                    data = json.loads(raw)
                except json.JSONDecodeError as e:
                    logger.warning(f"ws non-JSON frame ignored: {e!r}")
                    continue
                # T-03-04-01: log frame type only, NEVER the body
                logger.debug(f"ws event type={data.get('event_type', 'unknown')}")
                yield data
        except websockets.ConnectionClosed as e:
            # Outer reconnect-iterator will pick up; just log + continue.
            # Use modern .rcvd.code / .rcvd.reason (websockets 13.1+; .code/.reason deprecated).
            rcvd = getattr(e, "rcvd", None)
            code = getattr(rcvd, "code", None) if rcvd is not None else None
            reason = getattr(rcvd, "reason", None) if rcvd is not None else None
            logger.warning(
                f"ws connection closed code={code} reason={reason!r}; reconnecting…"
            )
            continue
        except asyncio.CancelledError:
            # F-04: must NOT be swallowed. SIGTERM relies on this.
            logger.info("ws_market_client: cancelled, closing socket and propagating")
            try:
                await ws.close()
            except Exception:  # noqa: BLE001
                pass
            raise
