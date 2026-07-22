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
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import sentry_sdk
import uvicorn
from loguru import logger

# All imports below are patched at IMPORT SITE (polyarb.daemon.l2_main.*)
# by tests — Phase 02 L9. Never patch at definition site.
from polyarb.config import load_settings
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.daemon.ws_watchdog import WsWatchdog
from polyarb.events.listener import listen_snapshot_complete
from polyarb.events.reconciliation import (
    AsyncpgCursorStore,
    ReconciliationPump,
    ReconciliationState,
)
from polyarb.http.l2_app import create_l2_app
from polyarb.observability.logging import init_logging
from polyarb.observability.sentry import init_sentry
from polyarb.observation.l2_candidate_refresh import on_snapshot_complete
from polyarb.observation.l3_evidence import (
    AcceptanceConfig,
    FrameDispatchResult,
    L3EvidenceRuntime,
    RuntimeBootRecord,
    RuntimeIdentity,
)
from polyarb.storage.l2_supabase_mirror import L2SupabaseMirror
from polyarb.storage.l3_evidence_store import L3EvidenceStore
from polyarb.storage.sqlite_store import SQLiteStore

_L3_RECIPE_PATH = Path(__file__).resolve().parents[1] / "scan_recipes" / "l3-promote.yaml"


@dataclass(frozen=True, slots=True)
class _L3EvidenceDependencies:
    """One immutable dependency graph shared by boot, WS, and promoter."""

    acceptance_config: AcceptanceConfig
    identity: RuntimeIdentity
    runtime: L3EvidenceRuntime
    boot: RuntimeBootRecord
    store: L3EvidenceStore
    runtime_dsn: str = field(repr=False)


def _build_l3_evidence_dependencies(
    *,
    settings: Any,
    recipe_yaml_path: Path,
    started_at: datetime | None = None,
) -> _L3EvidenceDependencies:
    """Construct L3 evidence dependencies without network or database I/O."""
    import polyarb

    effective_started_at = datetime.now(UTC) if started_at is None else started_at
    acceptance = AcceptanceConfig.from_settings(
        settings,
        recipe_yaml_path,
        code_version=polyarb.__version__,
    )
    identity = RuntimeIdentity(
        machine_id=os.environ.get("FLY_MACHINE_ID", "local"),
        machine_version=os.environ.get("FLY_MACHINE_VERSION", "local"),
        image_ref=os.environ.get("FLY_IMAGE_REF", "local"),
        release_id=settings.release_id,
        code_version=polyarb.__version__,
        recipe_sha256=acceptance.recipe_sha256,
        acceptance_config_hash=acceptance.digest(),
    )
    runtime = L3EvidenceRuntime(identity, started_at=effective_started_at)
    status = runtime.snapshot()
    boot = RuntimeBootRecord(
        boot_id=status.boot_id,
        started_at=status.started_at,
        machine_id=identity.machine_id,
        machine_version=identity.machine_version,
        image_ref=identity.image_ref,
        release_id=identity.release_id,
        code_version=identity.code_version,
        acceptance_config_hash=identity.acceptance_config_hash,
    )
    runtime_dsn = settings.l2_runtime_db_dsn.get_secret_value()
    return _L3EvidenceDependencies(
        acceptance_config=acceptance,
        identity=identity,
        runtime=runtime,
        boot=boot,
        store=L3EvidenceStore(runtime_dsn),
        runtime_dsn=runtime_dsn,
    )


async def _append_l3_boot(dependencies: _L3EvidenceDependencies) -> bool:
    """Append boot once and expose failure in runtime without blocking HTTP."""
    persisted = False
    if dependencies.runtime_dsn:
        try:
            persisted = await dependencies.store.append_boot(dependencies.boot)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - custom stores may raise
            logger.warning("l3 evidence boot append raised error_type={}", type(exc).__name__)
    at = datetime.now(UTC)
    dependencies.runtime.note_writer_result(
        persisted,
        at,
        "ok" if persisted else "boot_append_failed",
    )
    if not persisted:
        logger.warning("l3 evidence boot not persisted; promoter remains disabled")
    return persisted

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
    from datetime import datetime

    # Numeric path: unix seconds (or ms via the 1e12 heuristic).
    if isinstance(ts, (int, float)):
        try:
            ts_num = float(ts)
            if ts_num > 1e12:
                ts_num /= 1000.0
            return datetime.fromtimestamp(ts_num, tz=UTC).isoformat()
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
            return datetime.fromtimestamp(ts_num, tz=UTC).isoformat()
        except (TypeError, ValueError):
            pass
        try:
            iso_s = s.replace("Z", "+00:00") if s.endswith("Z") else s
            dt = datetime.fromisoformat(iso_s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt.astimezone(UTC).isoformat()
        except (TypeError, ValueError):
            return None

    return None


def _ranked_book_levels(
    levels: list,
    *,
    bids: bool,
) -> list[tuple[float, float]]:
    """Return valid ``(price, size)`` levels ranked nearest to the spread.

    Polymarket ``book`` frames have been observed in production with the
    farthest price first (bids ascending, asks descending).  Consumers must
    therefore rank by price instead of trusting array position.
    """
    ranked: list[tuple[float, float]] = []
    for entry in levels:
        if not isinstance(entry, dict):
            continue
        raw_price = entry.get("price")
        raw_size = entry.get("size")
        if raw_price is None or raw_size is None:
            continue
        try:
            price = float(raw_price)
            size = float(raw_size)
        except (TypeError, ValueError):
            continue
        if size <= 0:
            continue
        ranked.append((price, size))
    ranked.sort(key=lambda level: level[0], reverse=bids)
    return ranked


def _sum_depth_usd(
    levels: list,
    top_n: int = 10,
    *,
    bids: bool = True,
) -> float | None:
    """Sum (price * size) over price-ranked top-N orderbook levels.

    Quick task 260601: each level in the WS `book` event is `{"price": str, "size": str}`.
    Skip non-dict entries, non-numeric price/size, or size <= 0.

    Used by `_tob_row_from_frame` to populate `depth_yes_usd` (bids) and
    `depth_no_usd` (asks). Phase 05 L3 promoter D-13 threshold needs this.
    """
    ranked = _ranked_book_levels(levels, bids=bids)
    if not ranked:
        return None
    return sum(price * size for price, size in ranked[:top_n])


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
    # `book` frames carry bids/asks arrays. Production arrays can be ordered
    # farthest-first, so rank by price before selecting best/top-10 levels.
    # Quick 260601: also compute depth_yes_usd / depth_no_usd from top-10 levels.
    depth_yes_usd_v: float | None = None
    depth_no_usd_v: float | None = None
    if et == "book":
        bids = frame.get("bids") or []
        asks = frame.get("asks") or []
        ranked_bids = _ranked_book_levels(bids, bids=True)
        ranked_asks = _ranked_book_levels(asks, bids=False)
        if ranked_bids:
            best_bid = ranked_bids[0][0]
        if ranked_asks:
            best_ask = ranked_asks[0][0]
        depth_yes_usd_v = _sum_depth_usd(bids, top_n=10, bids=True)
        depth_no_usd_v = _sum_depth_usd(asks, top_n=10, bids=False)

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
        "depth_yes_usd": depth_yes_usd_v,
        "depth_no_usd": depth_no_usd_v,
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
    trade_hash = frame.get("trade_hash") or frame.get("transactionHash") or frame.get("txHash")
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


def _book_levels_rows_from_frame(frame: dict, max_levels: int = 10) -> list[dict]:
    """Project a WS ``book`` frame to up to ``2 * max_levels`` l2_book_levels rows.

    Phase 05 D-04 + D-07. Returns ``[]`` (never None, never raises) for:
    - frames without ``asset_id``
    - frames without ``timestamp`` (and no ``ts``)
    - books whose only entries are malformed (non-dict, non-numeric price)
    - books with all sizes ≤ 0

    Side normalization: bids → ``"BUY"``, asks → ``"SELL"`` (uppercase,
    consistent with ``l2_trades.side`` from :func:`_trade_row_from_frame`).

    Level numbering: 1-indexed AFTER filtering invalid entries and ranking by
    price (BUY descending, SELL ascending). This keeps level 1 equal to the
    executable best price regardless of the upstream array order.

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
        ranked = _ranked_book_levels(levels, bids=side_key == "bids")
        for valid_idx, (price, size) in enumerate(ranked[:max_levels], start=1):
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


def make_l2_event_handler(
    l2_mirror: L2SupabaseMirror | None,
    *,
    book_levels_required: Callable[[str], bool] | None = None,
) -> Callable[[dict], FrameDispatchResult]:
    """Build the production WS-frame dispatcher and expose mirror truth."""

    def _on_event(frame: dict) -> FrameDispatchResult:
        event_type = frame.get("event_type", "unknown")
        asset_id_raw = frame.get("asset_id") or ""
        observed_at_raw = _isoformat_ts(frame.get("timestamp") or frame.get("ts"))
        observed_at = (
            datetime.fromisoformat(observed_at_raw).astimezone(UTC)
            if observed_at_raw is not None
            else None
        )
        logger.debug(f"ws frame type={event_type} asset={asset_id_raw[:16]}")
        if l2_mirror is None:
            return FrameDispatchResult(False, False, observed_at)

        if event_type in ("price_change", "best_bid_ask", "book"):
            row = _tob_row_from_frame(frame)
            tob_written = False
            book_levels_written = False
            if row is not None:
                tob_written = l2_mirror.push_top_of_book([row]) is True
            if event_type == "book":
                if (
                    asset_id_raw
                    and book_levels_required is not None
                    and book_levels_required(str(asset_id_raw))
                ):
                    book_rows = _book_levels_rows_from_frame(frame, max_levels=10)
                    if book_rows:
                        book_levels_written = l2_mirror.push_book_levels(book_rows) is True
            return FrameDispatchResult(tob_written, book_levels_written, observed_at)
        if event_type == "last_trade_price":
            row = _trade_row_from_frame(frame)
            if row is not None:
                l2_mirror.push_trades([row])
        return FrameDispatchResult(False, False, observed_at)

    return _on_event


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

    # Construct one immutable evidence identity before any daemon-owned task.
    # This is deliberately side-effect-free: the HTTP server binds before the
    # role-preflighted boot append can touch PostgreSQL.
    l3_evidence = _build_l3_evidence_dependencies(
        settings=settings,
        recipe_yaml_path=_L3_RECIPE_PATH,
    )

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

    # The dispatcher is constructed before the consumer, but events cannot be
    # delivered until ``consumer.run`` starts.  Late binding therefore lets the
    # handler query the exact consumer instance without a global membership
    # surrogate or an initialization race.
    ws_consumer: Any = None
    _on_event = make_l2_event_handler(
        l2_mirror,
        book_levels_required=lambda asset_id: (
            ws_consumer is not None and ws_consumer.requires_book_levels(asset_id)
        ),
    )

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

    ws_consumer = WsConsumer(
        settings=settings,
        watchdog=watchdog,
        on_event=_on_event,
        initial_assets=_bootstrap_ids,
        membership_observer=l3_evidence.runtime.update_membership,
    )

    # Phase 05.1: NOTIFY is only a doorbell. One durable pump owns refresh and
    # cursor advancement; its initial wake replaces catch-up fan-out + sentinel
    # prime, and its timer keeps working while LISTEN is disconnected.
    reconciliation_state = ReconciliationState()
    dsn = l3_evidence.runtime_dsn

    async def _refresh_latest(payload: dict) -> bool:
        return await on_snapshot_complete(
            payload,
            ws_consumer=ws_consumer,
            settings=settings,
            mirror=l2_mirror,
        )

    reconciliation_pump = ReconciliationPump(
        store=AsyncpgCursorStore(dsn=dsn),
        refresh=_refresh_latest,
        state=reconciliation_state,
        poll_seconds=settings.event_reconcile_poll_seconds,
    )

    def _on_snapshot_notification(payload: dict) -> None:
        reconciliation_pump.notify(payload)

    app = create_l2_app(
        sqlite_store=sqlite_store,
        settings=settings,
        ws_consumer=ws_consumer,
        event_listener=reconciliation_state,
    )
    # Plan 03 will render strict public evidence checks.  Stashing the exact
    # runtime now makes failed/cold-start truth available without inventing a
    # second health identity in that later plan.
    app.state.l3_evidence_runtime = l3_evidence.runtime

    config = uvicorn.Config(
        app,
        host="0.0.0.0",  # Fly internal network only — fly-l2.toml controls exposure
        port=settings.http_port,
        log_config=None,  # use loguru, not uvicorn's logger
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
    watchdog_task: asyncio.Task[None] | None = None
    consumer_task: asyncio.Task[None] | None = None
    quiet_refresh_task: asyncio.Task[None] | None = None
    pump_task: asyncio.Task[None] | None = None
    listener_task: asyncio.Task[None] | None = None
    l3_promoter_task: asyncio.Task[None] | None = None
    l3_sampler_task: asyncio.Task[None] | None = None
    normal_shutdown = False
    try:
        # P9 server-started gate — MANDATORY per Phase 02 L5.  The task is
        # already inside this cleanup scope, so every timeout, serve failure,
        # and cancellation path observes it before returning.
        for _ in range(100):
            if server.started is True:
                break
            if server_task.done():
                await server_task
                raise RuntimeError("HTTP server exited before reporting started")
            await asyncio.sleep(0.1)
        else:
            raise TimeoutError("HTTP server did not start within the P9 gate")

        logger.info(
            f"polyarb-l2 daemon running: http on :{settings.http_port}, "
            f"variant={getattr(settings, 'daemon_variant', 'unknown')}"
        )

        # HTTP is reachable before boot role preflight touches PostgreSQL.
        # The successful boot append is the authorization capability for every
        # daemon-owned direct PostgreSQL task, not merely the promoter.
        l3_boot_persisted = await _append_l3_boot(l3_evidence)

        # WS + REST remain fail-soft when the dedicated runtime credential is
        # absent or rejected; neither path requires direct PostgreSQL access.
        watchdog_task = asyncio.create_task(watchdog.watch(stop_event))
        consumer_task = asyncio.create_task(ws_consumer.run(stop_event))
        quiet_refresh_task = asyncio.create_task(
            ws_consumer.run_quiet_refresh(stop_event), name="ws-quiet-refresh"
        )

        if l3_boot_persisted:
            pump_task = asyncio.create_task(
                reconciliation_pump.run(stop_event), name="reconciliation-pump"
            )
            listener_task = asyncio.create_task(
                listen_snapshot_complete(
                    dsn=dsn,
                    on_event=_on_snapshot_notification,
                    stop_event=stop_event,
                    state=reconciliation_state,
                ),
                name="snapshot-listener",
            )

            from polyarb.observation import l3_promote as l3_promote_module
            from polyarb.observation import l3_sampler as l3_sampler_module

            l3_promoter_task = asyncio.create_task(
                l3_promote_module.run_periodic(
                    stop_event=stop_event,
                    settings=settings,
                    ws_consumer=ws_consumer,
                    recipe_yaml_path=_L3_RECIPE_PATH,
                    evidence_store=l3_evidence.store,
                    evidence_runtime=l3_evidence.runtime,
                    acceptance_config=l3_evidence.acceptance_config,
                ),
                name="l3-promoter",
            )
            l3_sampler_task = asyncio.create_task(
                l3_sampler_module.run_sampler(
                    stop_event,
                    settings=settings,
                    ws_consumer=ws_consumer,
                    reconciliation_state=reconciliation_state,
                    runtime=l3_evidence.runtime,
                    store=l3_evidence.store,
                ),
                name="l3-evidence-sampler",
            )

        await stop_event.wait()
        normal_shutdown = True
    except asyncio.CancelledError:
        # F-04 contract — MUST propagate, not swallow.
        logger.info("polyarb-l2 daemon shutdown via CancelledError")
        raise
    finally:
        logger.info("polyarb-l2 daemon stopping")
        server.should_exit = True
        runtime_tasks = [
            (watchdog_task, "watchdog"),
            (consumer_task, "consumer"),
            (quiet_refresh_task, "ws-quiet-refresh"),
            (listener_task, "listener"),
            (pump_task, "reconciliation-pump"),
            (l3_promoter_task, "l3-promoter"),
            (l3_sampler_task, "l3-evidence-sampler"),
        ]
        if watchdog_task is not None:
            watchdog_task.cancel()
        if consumer_task is not None:
            consumer_task.cancel()
        if quiet_refresh_task is not None:
            quiet_refresh_task.cancel()
        if listener_task is not None:
            listener_task.cancel()
        if pump_task is not None:
            pump_task.cancel()
        if l3_promoter_task is not None:
            l3_promoter_task.cancel()
        if l3_sampler_task is not None:
            l3_sampler_task.cancel()
        if not normal_shutdown:
            server_task.cancel()

        # F-04 bounded shutdown — optional tasks remain optional because boot
        # authorization may fail before they are ever constructed.
        shutdown_tasks = [(server_task, "server")]
        shutdown_tasks.extend((task, name) for task, name in runtime_tasks if task is not None)
        for task, name in shutdown_tasks:
            try:
                await asyncio.wait_for(task, timeout=5.0)
            except TimeoutError:
                logger.warning(f"{name} task did not stop within 5s — forcing")
            except asyncio.CancelledError:
                # Expected for every explicitly-cancelled task.  Cancellation
                # of main itself is re-raised by the outer handler above.
                pass
            except Exception as exc:  # noqa: BLE001 - preserve primary failure
                logger.warning("{} task stopped with error_type={}", name, type(exc).__name__)

    logger.info("polyarb-l2 daemon stopped cleanly")
    return 0


def run() -> None:
    """Entry point for `python -m polyarb.daemon.l2_main`."""
    rc = asyncio.run(main())
    sys.exit(rc)


if __name__ == "__main__":
    run()
