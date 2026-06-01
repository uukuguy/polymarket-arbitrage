"""Polyarb L2 daemon entry — WS market channel + event listener + Starlette health.

Phase 03 Plan 03 — D-06: separate process from L1 snapshot daemon.
Phase 03 Plan 04 wired the real `WsWatchdog` + `WsConsumer` (replacing the
Mock-shaped placeholders). Plan 05 will wire `event_listener` to the
asyncpg-based event bus + candidate refresh.

Init order mirrors `polyarb.daemon.main` (Phase 02 P9 server-started gate is
MANDATORY — otherwise Fly's 120s grace period times out before uvicorn binds
the socket and the platform never observes a live port).

Run locally:
    POLYARB_DAEMON_VARIANT=l2 POLYARB_DB_PATH=./data/l2-state.db \
      POLYARB_HTTP_PORT=19081 uv run python -m polyarb.daemon.l2_main
    curl http://127.0.0.1:19081/health   # IETF strict
    curl http://127.0.0.1:19081/healthz  # always 200

Architecture:
    1. init_logging() — loguru JSON stdout (must be FIRST)
    2. load_settings() — pydantic-settings + .env
    3. init_sentry()   — AFTER logging (LoguruIntegration hook)
       + sentry_sdk.set_tag("service", "polyarb-l2")  — T-03-03-04 / cross-stream filter
    4. SQLiteStore(settings.db_path).init_schema()
    5. create_l2_app(...) — Starlette factory
    6. uvicorn.Server(...) — bound but not yet listening
    7. server_task = asyncio.create_task(server.serve())
    8. P9 server-started gate: for _ in range(100): if server.started: break
                                       else: await asyncio.sleep(0.1)
    9. await stop_event.wait() — signal-driven shutdown
    10. server.should_exit = True
    11. await asyncio.wait_for(server_task, timeout=5.0)  — F-04 bounded shutdown

Plan 05 placeholder: `event_listener` still starts as None; health check
renders "warn" with output="not_configured" until Plan 05 wires it.

Cross-pollination guard (T-03-03-03): this module MUST NOT import from
`polyarb.daemon.main`. Both files share the L1/L2 init contract via
parallel implementations; symbol sharing would obscure the separate
process boundary.
"""
from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from typing import Any

import sentry_sdk
import uvicorn
from loguru import logger

# All imports below are patched at IMPORT SITE (polyarb.daemon.l2_main.*)
# by tests — Phase 02 L9. Never patch at definition site.
from polyarb.config import load_settings
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.daemon.ws_watchdog import WsWatchdog
from polyarb.events.listener import catchup_from_cursor, listen_snapshot_complete
from polyarb.http.l2_app import create_l2_app
from polyarb.observability.logging import init_logging
from polyarb.observability.sentry import init_sentry
from polyarb.observation.l2_candidate_refresh import on_snapshot_complete
from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror
from polyarb.storage.sqlite_store import SQLiteStore


# ── Plan 06 D-07: WS frame → l2_* row builders ───────────────────────────
# Polymarket WS market-channel event shapes (empirical):
#   price_change:    {event_type, asset_id, price, side, size, timestamp, ...}
#   best_bid_ask:    {event_type, asset_id, best_bid, best_ask, timestamp, ...}
#   book:            {event_type, asset_id, bids:[...], asks:[...], timestamp, ...}
#   last_trade_price:{event_type, asset_id, market, price, side, size, ts,
#                     fee_rate_bps, transactionHash?, taker?, ...}
#
# These builders project each frame into the narrow l2_top_of_book /
# l2_trades schema. Missing fields → None (Postgres NULL). We never raise on
# malformed frames — return None and the dispatcher skips the write.


def _isoformat_ts(ts: int | float | str | None) -> str | None:
    """Normalize WS timestamp (unix seconds, ms, or ISO 8601 string) to ISO 8601 UTC.

    Phase 05 D-04: accepts ISO 8601 strings in addition to unix epoch numbers.
    The Polymarket ``book`` frame carries ``"timestamp": "2026-06-01T..."``
    per 05-CONTEXT.md <interfaces> block, while ``last_trade_price`` /
    ``price_change`` typically carry numeric ms. Both shapes route to the
    same normalized ISO 8601 UTC string for DB writes.

    Returns ``None`` on any parse failure (caller treats None as drop).
    """
    if ts is None:
        return None
    from datetime import datetime, timezone

    # Numeric path: unix seconds (or ms via the 1e12 heuristic).
    if isinstance(ts, (int, float)):
        try:
            ts_num = float(ts)
            if ts_num > 1e12:
                ts_num /= 1000.0
            return datetime.fromtimestamp(ts_num, tz=timezone.utc).isoformat()
        except Exception:
            return None

    # String path: try numeric-via-string first (some sources send "1717243200"),
    # then fall back to ISO 8601 parsing. Accept the trailing 'Z' alias for +00:00.
    if isinstance(ts, str):
        s = ts.strip()
        if not s:
            return None
        try:
            ts_num = float(s)
            if ts_num > 1e12:
                ts_num /= 1000.0
            return datetime.fromtimestamp(ts_num, tz=timezone.utc).isoformat()
        except (TypeError, ValueError):
            pass
        try:
            iso_s = s.replace("Z", "+00:00") if s.endswith("Z") else s
            dt = datetime.fromisoformat(iso_s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except (TypeError, ValueError):
            return None

    return None


def _tob_row_from_frame(frame: dict) -> dict | None:
    """Project a price_change / best_bid_ask / book frame to a l2_top_of_book row.

    Returns None if the frame lacks an asset_id (cannot index).
    """
    asset_id = frame.get("asset_id")
    if not asset_id:
        return None
    et = frame.get("event_type", "unknown")
    # best_bid_ask carries explicit best_bid/best_ask fields
    best_bid = frame.get("best_bid")
    best_ask = frame.get("best_ask")
    if best_bid is None and best_ask is None and et == "price_change":
        # price_change is per-side delta; carry side+price only as best_<side>
        side = (frame.get("side") or "").upper()
        price = frame.get("price")
        if side == "BUY":
            best_bid = price
        elif side == "SELL":
            best_ask = price
    # `book` frames carry bids/asks arrays — take top entry of each
    if et == "book":
        bids = frame.get("bids") or []
        asks = frame.get("asks") or []
        if bids and isinstance(bids[0], dict):
            best_bid = bids[0].get("price", best_bid)
        if asks and isinstance(asks[0], dict):
            best_ask = asks[0].get("price", best_ask)

    try:
        bb_f = float(best_bid) if best_bid is not None else None
        ba_f = float(best_ask) if best_ask is not None else None
    except (TypeError, ValueError):
        return None

    spread = (ba_f - bb_f) if (bb_f is not None and ba_f is not None) else None
    mid = ((bb_f + ba_f) / 2) if (bb_f is not None and ba_f is not None) else None

    return {
        "asset_id": asset_id,
        "ts": _isoformat_ts(frame.get("timestamp") or frame.get("ts")),
        "best_bid": bb_f,
        "best_ask": ba_f,
        "spread": spread,
        "mid_price": mid,
        "depth_yes_usd": None,  # populated only when book frame carries size
        "depth_no_usd": None,
        "source_event": et,
    }


def _trade_row_from_frame(frame: dict) -> dict | None:
    """Project a last_trade_price frame to a l2_trades row.

    Returns None if asset_id or trade_hash is missing (cannot dedup).
    """
    asset_id = frame.get("asset_id")
    if not asset_id:
        return None
    # Polymarket WS may key the tx hash under different names depending on
    # source; accept the common variants.
    trade_hash = (
        frame.get("trade_hash")
        or frame.get("transactionHash")
        or frame.get("txHash")
    )
    if not trade_hash:
        return None
    try:
        size = float(frame.get("size", 0))
    except (TypeError, ValueError):
        return None
    if size <= 0:
        return None
    return {
        "asset_id": asset_id,
        "market_id": frame.get("market") or frame.get("market_id"),
        "ts": _isoformat_ts(frame.get("timestamp") or frame.get("ts")),
        "price": frame.get("price"),
        "size": size,
        "side": (frame.get("side") or "").upper() or None,
        "taker_address": frame.get("taker") or frame.get("taker_address"),
        "trade_hash": trade_hash,
        "source": "ws",
    }


def _book_levels_rows_from_frame(
    frame: dict, max_levels: int = 10
) -> list[dict]:
    """Project a WS ``book`` frame to up to ``2 * max_levels`` l2_book_levels rows.

    Phase 05 D-04 + D-07. Returns ``[]`` (never None, never raises) for:
    - frames without ``asset_id``
    - frames without ``timestamp`` (and no ``ts``)
    - books whose only entries are malformed (non-dict, non-numeric price)
    - books with all sizes ≤ 0

    Side normalization: bids → ``"BUY"``, asks → ``"SELL"`` (uppercase,
    consistent with ``l2_trades.side`` from :func:`_trade_row_from_frame`).

    Level numbering: 1-indexed AFTER filtering invalid entries. The first
    valid bid is level=1 even if earlier dict entries had size=0 — this
    keeps the (asset_id, ts, side, level) UNIQUE constraint stable across
    snapshots where the worst level happens to be temporarily zeroed.

    ``max_levels=10`` (D-07) caps each side at top-10 → up to 20 rows per
    book event per asset. Bounded by design — bigger payloads from the
    Polymarket WS are silently truncated.

    Returned dict shape matches ``_NARROW_BOOK_LEVELS_COLUMNS`` exactly:
    ``{asset_id, ts, side, level, price, size}``.
    """
    asset_id = frame.get("asset_id")
    if not asset_id:
        return []
    ts_iso = _isoformat_ts(frame.get("timestamp") or frame.get("ts"))
    if ts_iso is None:
        return []
    rows: list[dict] = []
    for side_key, side_norm in (("bids", "BUY"), ("asks", "SELL")):
        levels = frame.get(side_key) or []
        valid_idx = 0
        for entry in levels:
            if valid_idx >= max_levels:
                break
            if not isinstance(entry, dict):
                continue
            raw_price = entry.get("price")
            raw_size = entry.get("size", 0)
            if raw_price is None:
                continue
            try:
                price = float(raw_price)
                size = float(raw_size) if raw_size is not None else 0.0
            except (TypeError, ValueError):
                continue
            if size <= 0:
                continue
            valid_idx += 1
            rows.append(
                {
                    "asset_id": asset_id,
                    "ts": ts_iso,
                    "side": side_norm,
                    "level": valid_idx,
                    "price": price,
                    "size": size,
                }
            )
    return rows


class _EventListenerWrapper:
    """Health-surface shim — health endpoint reads is_listening + last event ts.

    Plan 05 D-05: the listener task itself flips is_listening=True after a
    successful LISTEN snapshot_complete; the dispatch wrapper bumps
    last_event_received_s on each NOTIFY received.
    """

    def __init__(self) -> None:
        self.is_listening: bool = False
        self.last_event_received_s: float = time.time()


async def main() -> int:
    # 1. FIRST — sets up JSON stdout sink + InterceptHandler
    init_logging()

    # 2. config (load_settings reads pydantic env + YAML overrides)
    settings = load_settings()
    if getattr(settings, "daemon_variant", "l1") != "l2":
        logger.warning(
            f"daemon_variant={settings.daemon_variant!r} but l2_main.py invoked; "
            f"proceeding (POLYARB_DAEMON_VARIANT env override missing?)"
        )

    # 3. sentry AFTER logging (LoguruIntegration needs the loguru sink installed)
    init_sentry(settings)
    # Phase 03 Plan 03 — differentiate polyarb-l2 from polyarb-l1 in Sentry stream
    # so cross-service event filtering works. T-03-03-04 mitigation: literal string.
    sentry_sdk.set_tag("service", "polyarb-l2")

    logger.info("polyarb-l2 daemon starting up")

    # Phase 03.1-06 D-04: loud warning if chaos kill flag is set at startup.
    # Makes accidental prod-set immediately visible in flyctl logs. Paired with
    # the chaos:ws_test_kill_flag sub-check in /health (l2_health.py) for
    # chain-truth own-dog-food per feedback_code-vs-chain-truth-2026-05.
    if os.getenv("POLYARB_WS_TEST_KILL") == "1":
        logger.warning(
            "⚠ POLYARB_WS_TEST_KILL=1 detected — CHAOS MODE; WS will drop on "
            "next message. This MUST NOT appear in production."
        )

    # 4. SQLite (separate DB path from L1 — settings.db_path = /data/l2-state.db on Fly)
    sqlite_store = SQLiteStore(settings.db_path)
    sqlite_store.init_schema()

    # ── Plan 04 wiring (real WsWatchdog + WsConsumer) ────────────────────────
    # Plan 05 wired event_listener; Plan 06 replaces placeholder on_event with
    # real L2SupabaseMirror dispatch by event_type.
    watchdog = WsWatchdog(stale_s=30.0)

    # ── Plan 06: L2SupabaseMirror init (D-07) — fail-soft if creds missing ──
    # Phase 03.1 Plan 02 (B-2 chain-truth): pass store=sqlite_store so the
    # mirror writes a freshness anchor to l2_mirror_state on every successful
    # push. /health mirror:l2_tob_age_seconds sub-check reads that anchor →
    # chain-truth alive: code → DB → /health → operator alert.
    l2_mirror: L2SupabaseMirror | None
    if settings.supabase_url and settings.supabase_service_key.get_secret_value():
        l2_mirror = L2SupabaseMirror(
            url=settings.supabase_url,
            service_key=settings.supabase_service_key.get_secret_value(),
            store=sqlite_store,
        )
        logger.info("l2-mirror enabled (Supabase REST URL + service_key present)")
    else:
        l2_mirror = None
        logger.info("l2-mirror disabled (POLYARB_SUPABASE_URL or _SERVICE_KEY missing)")
        # Phase 02.1 P1 — double-anchor for disabled path so dashboards know
        # at-a-glance whether the silence is by design or by failure.
        sentry_sdk.add_breadcrumb(
            category="l2-mirror",
            level="info",
            message="l2-mirror disabled (config); no l2_* writes will occur",
        )

    def _on_event(frame: dict) -> None:
        """Plan 06 D-07: dispatch WS frame to L2SupabaseMirror by event_type.

        T-03-04-01 mitigation: log only event_type + asset prefix, never body.
        Mirror is fail-soft — call returns False on failure but never raises.
        """
        event_type = frame.get("event_type", "unknown")
        asset_id_raw = frame.get("asset_id") or ""
        logger.debug(f"ws frame type={event_type} asset={asset_id_raw[:16]}")
        if l2_mirror is None:
            return  # disabled — startup breadcrumb already explains why

        if event_type in ("price_change", "best_bid_ask", "book"):
            row = _tob_row_from_frame(frame)
            if row is not None:
                l2_mirror.push_top_of_book([row])
        elif event_type == "last_trade_price":
            row = _trade_row_from_frame(frame)
            if row is not None:
                l2_mirror.push_trades([row])

    # Bootstrap asset_ids from env (Phase 03 Wave 5 deploy aid 2026-05-25):
    # without this, L2 cold-starts with empty subscribed_assets and idles
    # until L1 emits NOTIFY (which requires event_bus_enabled=True per B1).
    # In debug/dev mode, set POLYARB_BOOTSTRAP_ASSET_IDS=<id1>,<id2>,... to
    # have WS connect immediately. candidate_refresh diffs against this set.
    _bootstrap_ids = [
        s.strip() for s in (settings.bootstrap_asset_ids or "").split(",") if s.strip()
    ]
    if _bootstrap_ids:
        logger.info(
            f"ws_consumer: bootstrapping with {len(_bootstrap_ids)} asset_ids "
            f"from POLYARB_BOOTSTRAP_ASSET_IDS"
        )

    ws_consumer: Any = WsConsumer(
        settings=settings,
        watchdog=watchdog,
        on_event=_on_event,
        initial_assets=_bootstrap_ids,
    )

    # ── Plan 05 wires: EventListener + on_snapshot_complete dispatch ─────
    event_listener: Any = _EventListenerWrapper()

    def _dispatch_on_snapshot(payload: dict) -> None:
        """Sync-callback bridge from asyncpg loop to async refresh handler.

        Plan 06: pass `mirror=l2_mirror` so candidate refresh persists
        diff to l2_candidates (D-07 dashboard write path).
        """
        event_listener.last_event_received_s = time.time()
        try:
            asyncio.create_task(
                on_snapshot_complete(
                    payload,
                    ws_consumer=ws_consumer,
                    settings=settings,
                    mirror=l2_mirror,
                )
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"on_snapshot_complete dispatch failed: {e!r}")
    # ─────────────────────────────────────────────────────────────────────────

    app = create_l2_app(
        sqlite_store=sqlite_store,
        settings=settings,
        ws_consumer=ws_consumer,
        event_listener=event_listener,
    )

    config = uvicorn.Config(
        app,
        host="0.0.0.0",   # Fly internal network only — fly-l2.toml controls exposure
        port=settings.http_port,
        log_config=None,   # use loguru, not uvicorn's logger
        access_log=False,  # Axiom doesn't need access logs at this volume
        loop="asyncio",
    )
    server = uvicorn.Server(config)

    stop_event = asyncio.Event()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info(f"polyarb-l2 received {sig.name}, initiating graceful shutdown")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown, sig)
        except NotImplementedError:
            # Windows fallback — never hit on Fly Linux but keeps test-host portable
            pass

    server_task = asyncio.create_task(server.serve())

    # P9 server-started gate — MANDATORY per Phase 02 L5.
    # uvicorn must bind the socket BEFORE any long-running task (WS / event
    # listener) starts, else Fly's 120s grace period times out and the
    # machine never gets to handle a real request.
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.1)
    logger.info(
        f"polyarb-l2 daemon running: http on :{settings.http_port}, "
        f"variant={getattr(settings, 'daemon_variant', 'unknown')}"
    )

    # Plan 04 wiring — watchdog + consumer tasks alongside server_task.
    # Plan 05 wiring — event listener task (asyncpg LISTEN) + startup catchup.
    watchdog_task = asyncio.create_task(watchdog.watch(stop_event))
    consumer_task = asyncio.create_task(ws_consumer.run(stop_event))

    # ── Plan 05: startup catch-up (Plan 06 cursor table absence tolerated) ──
    # P0 fix (2026-05-25): catchup must REPLAY each missed snapshot through the
    # dispatch chain, then advance l2_event_cursor.last_snapshot_id so the next
    # restart is monotonic. Prior code only logged the count; missed snapshots
    # silently never reached candidate_refresh, leaving WS subscribed_assets
    # empty on every cold start.
    try:
        dsn = settings.supabase_db_dsn.get_secret_value()
        if dsn:
            missed = await catchup_from_cursor(dsn=dsn, consumer="l2-candidate-refresh")
            if missed:
                logger.info(
                    f"event-bus catchup: replaying {len(missed)} missed snapshots"
                )
                for row in missed:
                    _dispatch_on_snapshot(
                        {"snapshot_id": row["id"], "ts_s": row["taken_at_ms"] / 1000.0}
                    )
                # Advance cursor — best-effort; cursor table is in Supabase, so
                # use the same dsn. fail-soft on connection error.
                try:
                    import asyncpg as _asyncpg
                    _conn = await _asyncpg.connect(dsn=dsn)
                    try:
                        await _conn.execute(
                            "INSERT INTO l2_event_cursor (consumer, last_snapshot_id) "
                            "VALUES ($1, $2) ON CONFLICT (consumer) DO UPDATE "
                            "SET last_snapshot_id = EXCLUDED.last_snapshot_id",
                            "l2-candidate-refresh",
                            int(missed[-1]["id"]),
                        )
                        logger.info(
                            f"event-bus catchup: cursor advanced to snapshot_id={missed[-1]['id']}"
                        )
                    finally:
                        await _conn.close()
                except Exception as e:  # noqa: BLE001
                    logger.warning(
                        f"event-bus catchup: cursor advance failed (fail-soft): {e!r}"
                    )
            else:
                logger.info("event-bus catchup: no missed snapshots")
        else:
            logger.info("event-bus catchup skipped: supabase_db_dsn not set")
    except Exception as e:  # noqa: BLE001
        logger.warning(
            f"catchup_from_cursor failed (fail-soft, Plan 06 may not have shipped yet): {e!r}"
        )

    # ── Phase 04.1 G-02: eager startup-prime (D-01.1/D-01.2) ────────────────
    # Catchup-with-no-missed leaves candidate set at bootstrap ids only
    # (04-SOAK-LOG §G-02). Force ONE markets_latest fetch so the WS subscribes
    # to the real candidate set on every cold start, independent of missed
    # count. Additive — does NOT replace D-04 bootstrap fallback (bootstrap_ids
    # already drive WS before this fires). Sentinel snapshot_id=-1 is safe:
    # on_snapshot_complete uses snapshot_id only for log lines, never a row read.
    # G-01 fix (39c60ef) guarantees this first call passes the refresh debounce.
    _dispatch_on_snapshot(
        {"snapshot_id": -1, "_startup_prime": True, "ts_s": time.time()}
    )
    logger.info("event-bus startup-prime dispatched (G-02 cross-restart robustness)")

    # ── Plan 05: long-running listener task ─────────────────────────────────
    async def _listener_runner() -> None:
        """Wrap listen_snapshot_complete so health wrapper flips is_listening."""
        try:
            event_listener.is_listening = True
            dsn = settings.supabase_db_dsn.get_secret_value()
            if not dsn:
                logger.warning(
                    "event listener: supabase_db_dsn not set; idling until shutdown"
                )
                await stop_event.wait()
                return
            await listen_snapshot_complete(
                dsn=dsn, on_event=_dispatch_on_snapshot, stop_event=stop_event
            )
        finally:
            event_listener.is_listening = False

    listener_task = asyncio.create_task(_listener_runner())

    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        # F-04 contract — MUST propagate, not swallow.
        logger.info("polyarb-l2 daemon shutdown via CancelledError")
        raise
    finally:
        logger.info("polyarb-l2 daemon stopping")
        server.should_exit = True
        # Signal watchdog + consumer + listener to exit by cancelling explicitly
        # (stop_event is already set, but cancel is the belt-and-suspenders for
        # any blocking await).
        watchdog_task.cancel()
        consumer_task.cancel()
        listener_task.cancel()
        # F-04 bounded shutdown — even if any task ignores cancel, exit within 5s each
        for task, name in (
            (server_task, "server"),
            (watchdog_task, "watchdog"),
            (consumer_task, "consumer"),
            (listener_task, "listener"),
        ):
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning(f"{name} task did not stop within 5s — forcing")
            except asyncio.CancelledError:
                # Expected for watchdog_task / consumer_task because we cancelled them.
                # Re-raise only if it's the server (graceful exit signal).
                if name == "server":
                    raise

    logger.info("polyarb-l2 daemon stopped cleanly")
    return 0


def run() -> None:
    """Entry point for `python -m polyarb.daemon.l2_main`."""
    rc = asyncio.run(main())
    sys.exit(rc)


if __name__ == "__main__":
    run()
