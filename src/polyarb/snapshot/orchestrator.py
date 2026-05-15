"""Snapshot orchestrator — wires Gamma + CLOB + validator + storage together.

The 7-step pipeline:

    1. Gamma fetch       — /events first (for category/tags), then /markets
                           (Phase 1.1 Amendment 01: dual-source fetch)
    2. Normalize         — Gamma raw → storage row contract (drop unrecoverable);
                           markets get event_id FK from events' nested markets list
    3. Mode filter       — subset (liquidity>$1k) | full (all markets)
    4. CLOB batch fetch  — order books + buy/sell prices for the filtered tokens
    5. Stamp + attach    — fetched_at_ms (Pitfall 6) + best_bid/best_ask top-of-book
    6. Validate          — Layer 1 count, Layer 2 fields, Layer 4 cross-source
    7. Persist           — Parquet atomic write FIRST, then SQLite single-tx write
                           (snapshots → events → event_tags → markets FK order)

Every step records errors as ``Issue`` objects rather than raising. The
orchestrator NEVER calls ``sys.exit`` — the CLI is responsible for setting the
process exit code based on ``SnapshotResult.is_valid``.

Failure semantics (D-D3 / D-E2):
    - Gamma unreachable      → Layer 1 Issue(API_UNREACHABLE), proceed with []
    - CLOB unreachable       → Layer 4 Issue(API_UNREACHABLE), proceed without books
    - Validation issue found → row still persisted with is_valid flag derived
                                from is_valid_overall(issues) (Layer 1 only flips it
                                in Phase 1)

Security invariants applied:
    - F-1: every float() coercion of CLOB book prices/sizes is wrapped in
      try/except (KeyError, TypeError, ValueError, IndexError); failures are
      surfaced as Issue(layer=4, category=UNKNOWN) with raw_payload truncated.
    - F-5: exception details capped at 200 chars; book payloads at 500 bytes.

Phase 1 simplifications (documented for Phase 2 cleanup):
    - top-of-book attached only for ``yes_token_id`` (NO side is symmetric
      on Polymarket; Layer 4 validator still checks both tokens for ghost-book).
    - ``fetched_at_ms`` is stamped on ALL normalized markets including those
      filtered out of the subset (semantic gap from F-1 review — see SUMMARY).
"""

from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from loguru import logger


def _format_elapsed(seconds: float) -> str:
    """Format a duration as '12.3s' / '1m 23s' / '1h 02m 03s' for log readability."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    if seconds < 3600:
        m = int(seconds // 60)
        s = int(seconds % 60)
        return f"{m}m {s:02d}s"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    s = int(seconds % 60)
    return f"{h}h {m:02d}m {s:02d}s"


@contextmanager
def _phase(label: str):
    """Bracket a pipeline phase with start/done log lines and elapsed timing.

    The 'done' line uses a ► glyph so post-run grep can isolate phase summaries:
        grep '► Phase' /tmp/snap.log
    """
    logger.info(f"Phase {label} — start")
    t0 = time.monotonic()
    try:
        yield
    finally:
        logger.info(f"► Phase {label} — done in {_format_elapsed(time.monotonic() - t0)}")

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.clients.gamma_client import GammaClient
from polyarb.config import Settings
from polyarb.snapshot.cache import ChunkCache
from polyarb.snapshot.normalizer import normalize_events, normalize_market
from polyarb.storage.parquet_writer import compute_snapshot_path, write_parquet_atomic
from polyarb.storage.sqlite_store import SQLiteStore
from polyarb.validator.category import Category, Issue
from polyarb.validator.layers import (
    determine_snapshot_status,
    is_valid_overall,
    layer1_count,
    layer2_fields,
    layer4_cross_source,
)


@dataclass
class SnapshotResult:
    """Return value of ``run_snapshot`` — what the CLI prints + what tests assert on.

    """

    snapshot_id: int
    market_count: int
    is_valid: bool
    status: str  # "ok" / "degraded" / "failed" (SnapshotStatus enum value)
    mode: str
    issue_count: int
    issue_categories: dict[str, int]  # {category_value: count}
    parquet_path: Path
    taken_at_ms: int
    finished_at_ms: int


def _index_books_by_token(books: list) -> dict[str, dict]:
    """Map CLOB ``OrderBookSummary`` objects to ``{token_id: book_dict}``.

    The token-id field is ``asset_id`` per 01-2-SUMMARY (resolved empirically).
    Falls back to ``market`` / ``token_id`` so a future SDK rename doesn't
    silently drop books. Each book is normalized into a plain dict (the SDK
    object is dataclass-like with ``__dict__``).
    """
    out: dict[str, dict] = {}
    for b in books:
        if b is None:
            continue
        # OrderBookSummary is dataclass-like — pull __dict__ if available, else
        # treat as already-a-dict (test mocks may pass plain dicts directly).
        bd: dict = b.__dict__ if hasattr(b, "__dict__") and not isinstance(b, dict) else b
        tid = bd.get("asset_id") or bd.get("market") or bd.get("token_id")
        if tid:
            out[str(tid)] = bd
    return out


async def run_snapshot(
    settings: Settings,
    *,
    mode: str = "subset",
    now_ms: int | None = None,
    use_cache: bool = True,
) -> SnapshotResult:
    """Run one Polymarket snapshot end-to-end.

    Args:
        settings: Plan-1 ``Settings`` (URLs, rate caps, retry knobs, paths).
        mode: ``"subset"`` (default; only liquidity_usd > threshold)
              or ``"full"`` (all markets).
        now_ms: Override for the snapshot's ``taken_at_ms`` timestamp (test hook).
                Defaults to ``int(time.time() * 1000)`` at function entry.

    Returns:
        SnapshotResult — never raises for transport failures (those become
        Issues). Re-raises only for unexpected internal errors (e.g. SQLite
        rollback, Parquet schema mismatch — these should never happen with
        the normalizer's contract).

    use_cache:
        When True (default) the CLOB chunk cache (``settings.cache_root``)
        is consulted at startup; an in-progress run that died mid-CLOB can
        resume from the last completed chunk. The cache is cleaned up on
        successful persistence (step 7). When False, all existing caches
        under ``cache_root`` are purged at start and chunks are not saved.
    """
    if mode not in ("subset", "full"):
        raise ValueError(f"invalid mode: {mode!r} (must be 'subset' or 'full')")

    taken_at_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    issues: list[Issue] = []
    gamma_count_reported: int | None = None
    raw_markets: list[dict] = []
    raw_events: list[dict] = []
    event_rows: list[dict] = []
    event_tag_rows: list[dict] = []
    market_to_event_map: dict[str, str] = {}

    logger.info(
        f"snapshot starting — mode={mode}, cache={'on' if use_cache else 'off'}, "
        f"taken_at_ms={taken_at_ms}"
    )

    overall_t0 = time.monotonic()

    # ── 1. Gamma fetch (events FIRST for category/tags, then markets) ─────────
    # Phase 1.1 Amendment 01: /events is the only source of category/tags. We
    # fetch /events first to build market→event_id reverse map, then /markets,
    # so the normalizer can stamp event_id on every market row.
    # If /events fails we still proceed with /markets (event_id will be None for
    # all markets — graceful degradation, single Issue recorded).
    with _phase("1/7: Gamma fetch (active events + markets)"):
        async with GammaClient(settings) as gamma:
            # Step 1a: events (best-effort — failure → empty map)
            try:
                raw_events = await gamma.fetch_all_active_events()
                logger.info(f"Gamma: fetched {len(raw_events)} active events")
            except Exception as e:  # noqa: BLE001 — categorize, do NOT propagate
                logger.error(f"Gamma /events fetch failed: {e!r}")
                issues.append(
                    Issue(
                        layer=1,
                        category=Category.API_UNREACHABLE,
                        market_id=None,
                        detail=f"Gamma /events unreachable: {str(e)[:200]}",
                    )
                )
                raw_events = []

            # Step 1b: markets (mainline — failure → empty markets)
            try:
                raw_markets = await gamma.fetch_all_active_markets()
                gamma_count_reported = len(raw_markets)
                logger.info(f"Gamma: fetched {gamma_count_reported} active markets")
            except Exception as e:  # noqa: BLE001 — categorize, do NOT propagate
                logger.error(f"Gamma /markets fetch failed: {e!r}")
                issues.append(
                    Issue(
                        layer=1,
                        category=Category.API_UNREACHABLE,
                        market_id=None,
                        detail=f"Gamma unreachable: {str(e)[:200]}",
                    )
                )
                raw_markets = []

    # ── 2. Normalize (events first to build map, then markets with map injection)
    with _phase("2/7: Normalize + dedupe"):
        # Step 2a: events → events_rows + event_tags + market→event_id map
        event_rows, event_tag_rows, market_to_event_map = normalize_events(raw_events)
        del raw_events  # free 10k+ raw Gamma dicts immediately
        logger.info(
            f"Events normalized: {len(event_rows)} events, "
            f"{len(event_tag_rows)} event_tags, "
            f"{len(market_to_event_map)} market→event mappings"
        )

        # Step 2b: markets, injecting event_id from the reverse map
        raw_market_count = len(raw_markets)
        markets: list[dict] = [
            m
            for m in (
                normalize_market(r, market_to_event_map) for r in raw_markets
            )
            if m is not None
        ]
        del raw_markets  # free 10k+ raw Gamma dicts immediately

        # Dedupe by market_id — Gamma /markets returns ~4% duplicates across pagination
        # boundaries (live empirical: 1,960 dups in 48,985 rows on 2026-04-29). The only
        # observed differing field is liquidity_usd (drifts between page fetches), so
        # keeping the FIRST occurrence is safe and stable. Without this, SQLite's UNIQUE
        # constraint on markets.market_id rolls back the entire snapshot insert.
        seen_ids: set[str] = set()
        deduped: list[dict] = []
        for m in markets:
            mid = m.get("market_id")
            if mid is None or mid in seen_ids:
                continue
            seen_ids.add(mid)
            deduped.append(m)
        dup_count = len(markets) - len(deduped)
        if dup_count > 0:
            logger.info(f"Deduped {dup_count} markets by market_id (Gamma pagination overlap)")
        markets = deduped
        logger.info(f"Normalized: {len(markets)}/{raw_market_count} unique markets kept")

    # ── 3. Mode filter → token list ───────────────────────────────────────────
    with _phase("3/7: Mode filter"):
        if mode == "subset":
            target_markets = [
                m for m in markets
                if (m.get("liquidity_usd") or 0) > settings.liquidity_threshold_usd
            ]
        else:
            target_markets = markets

        token_ids: list[str] = []
        for m in target_markets:
            for k in ("yes_token_id", "no_token_id"):
                tid = m.get(k)
                if tid:
                    token_ids.append(tid)

        logger.info(
            f"Mode={mode}: {len(target_markets)}/{len(markets)} markets, "
            f"{len(token_ids)} tokens to fetch from CLOB"
        )

    # ── 4. CLOB batch fetch (best-effort: failure → Issue, not raise) ─────────
    # Cache wires in here: try_resume() either rebinds to a reusable cache from
    # a prior interrupted run (matching settings + token list + age <30min) or
    # initializes a fresh dir. ChunkCache is per-(taken_at_ms, token_set), so an
    # entirely fresh run with new tokens never accidentally reuses stale data.
    books_by_token: dict[str, dict] = {}
    prices_buy: dict = {}
    prices_sell: dict = {}
    cache: ChunkCache | None = None
    with _phase("4/7: CLOB fetch (books + buy/sell prices)"):
        if use_cache:
            cache = ChunkCache(
                cache_root=settings.cache_root,
                taken_at_ms=taken_at_ms,
                settings=settings,
                token_ids=token_ids,
                mode=mode,
            )
            cache.try_resume()
            # If we resumed an older cache, its taken_at_ms differs from ours.
            # We DON'T adopt the cached taken_at_ms — the run's taken_at_ms is
            # the moment THIS run started, used for parquet path + DB row.
            # Cache is just intermediate IO; final timestamps stay fresh.
        else:
            purged = ChunkCache.purge_all(settings.cache_root)
            if purged > 0:
                logger.info(f"--no-cache: purged {purged} cache directories")

        clob = ClobReaderClient(settings)
        try:
            books = await clob.get_books(token_ids, cache=cache)
            prices = await clob.get_prices_buy_sell(token_ids, cache=cache)
            prices_buy = prices.get("buy", {})
            prices_sell = prices.get("sell", {})
            books_by_token = _index_books_by_token(books)
            logger.info(
                f"CLOB: {len(books_by_token)} books indexed, "
                f"{len(prices_buy)}/{len(prices_sell)} buy/sell prices"
            )
        except Exception as e:  # noqa: BLE001 — categorize, do NOT propagate
            logger.error(f"CLOB fetch failed: {e!r}")
            # F-5: cap exception detail to 200 chars.
            issues.append(
                Issue(
                    layer=4,
                    category=Category.API_UNREACHABLE,
                    market_id=None,
                    detail=f"CLOB unreachable: {str(e)[:200]}",
                )
            )

    # ── 5. Stamp fetched_at_ms + attach top-of-book (yes side; F-1 wrapped) ──
    # Only target_markets are persisted, so only stamp/attach those. Filtered-out
    # markets stay in `markets` for layer-1 count comparison only — we never
    # write them anywhere. (Closes the "fetched_at_ms semantically wrong on
    # filter-excluded rows" gap from 01-4-SUMMARY.)
    clob_done_ms = int(time.time() * 1000)
    with _phase("5/7: Stamp + attach top-of-book"):
        for m in target_markets:
            m["fetched_at_ms"] = clob_done_ms

            # Attach top-of-book using yes_token_id only (single-side row).
            tid = m.get("yes_token_id")
            if not tid or tid not in books_by_token:
                continue
            book = books_by_token[tid]
            asks = book.get("asks") or []
            bids = book.get("bids") or []

            # F-1 SECURITY: CLOB book is attacker-controlled external input.
            # Malformed price/size strings (NaN, missing key, null) must NOT crash
            # the snapshot — log as Issue(layer=4, category=UNKNOWN) and continue.
            # Honors D-D3 (校验失败仍落库). raw_payload truncated to 500 bytes (F-5).
            if asks:
                try:
                    m["best_ask_price"] = float(asks[0]["price"])
                    m["best_ask_size"] = float(asks[0]["size"])
                except (KeyError, TypeError, ValueError, IndexError) as e:
                    issues.append(
                        Issue(
                            layer=4,
                            category=Category.UNKNOWN,
                            market_id=m.get("market_id"),
                            detail=f"unparseable ask for {tid}: {str(e)[:200]}",
                            raw_payload=json.dumps(book, default=str)[:500],
                        )
                    )
            if bids:
                try:
                    m["best_bid_price"] = float(bids[0]["price"])
                    m["best_bid_size"] = float(bids[0]["size"])
                except (KeyError, TypeError, ValueError, IndexError) as e:
                    issues.append(
                        Issue(
                            layer=4,
                            category=Category.UNKNOWN,
                            market_id=m.get("market_id"),
                            detail=f"unparseable bid for {tid}: {str(e)[:200]}",
                            raw_payload=json.dumps(book, default=str)[:500],
                        )
                    )

    # ── 6. Validate (Layer 1 / 2 / 4) ─────────────────────────────────────────
    with _phase("6/7: Validate (Layer 1/2/4)"):
        if gamma_count_reported is not None:
            # Layer 1 compares Gamma's reported active count vs how many we kept
            # post-normalize. A diff means either a bug in normalize OR API jitter.
            issues.extend(layer1_count(gamma_count_reported, len(markets)))

        # Layer 2/4 validate ONLY persisted markets. Filtered-out markets aren't
        # part of this snapshot's "completeness" claim — they'd flood
        # validation_issues with thousands of phantom warnings.
        issues.extend(layer2_fields(target_markets, now_ms=taken_at_ms))

        # Layer 4 expects {token_id: {"buy": <price-as-str-or-num>, "sell": ...}}
        # The CLOB SDK gives us {tid: {"BUY": "0.46"}} on each side — unwrap that
        # inner side-keyed dict so the validator can _safe_float() the value
        # directly. This shape contract is verified by validator tests.
        all_tids = set(prices_buy) | set(prices_sell)

        def _unwrap_side(side_dict: dict | None, key: str) -> str | None:
            if not isinstance(side_dict, dict):
                return None
            return side_dict.get(key)

        prices_combined = {
            tid: {
                "buy": _unwrap_side(prices_buy.get(tid), "BUY"),
                "sell": _unwrap_side(prices_sell.get(tid), "SELL"),
            }
            for tid in all_tids
        }
        issues.extend(layer4_cross_source(target_markets, books_by_token, prices_combined))

        status = determine_snapshot_status(issues)
        is_valid = is_valid_overall(issues)  # True for OK/DEGRADED, False for FAILED
        logger.info(
            f"Validated: status={status.value}, is_valid={is_valid}, {len(issues)} total issues "
            f"({sum(1 for i in issues if i.layer == 1)} L1, "
            f"{sum(1 for i in issues if i.layer == 2)} L2, "
            f"{sum(1 for i in issues if i.layer == 4)} L4)"
        )

    # ── 7. Persist (Parquet atomic FIRST, then SQLite single-tx) ──────────────
    finished_at_ms = int(time.time() * 1000)
    parquet_path = compute_snapshot_path(settings.parquet_root, taken_at_ms)
    with _phase("7/7: Persist (Parquet then SQLite)"):
        # Build parquet rows — must match SNAPSHOT_SCHEMA (22 fields including
        # snapshot_taken_at_ms + snapshot_id parquet-only fields). Persist ONLY
        # target_markets (the mode-filtered set). Filtered-out markets aren't part
        # of this snapshot's claim.
        parquet_rows: list[dict] = []
        for m in target_markets:
            row = dict(m)
            row["snapshot_taken_at_ms"] = taken_at_ms
            row["snapshot_id"] = 0  # placeholder — Parquet has no FK; SQLite assigns the real id
            row.setdefault("fetched_at_ms", clob_done_ms)
            parquet_rows.append(row)
        write_parquet_atomic(parquet_rows, parquet_path)

        # Phase 1.1 Amendment 01: stamp events with finished_at_ms (NOT
        # clob_done_ms — events are fetched by Gamma in phase 1, not CLOB).
        # We use finished_at_ms as the conventional "events landed at" time.
        for ev in event_rows:
            if ev.get("fetched_at_ms") is None:
                ev["fetched_at_ms"] = finished_at_ms

        store = SQLiteStore(settings.db_path)
        store.init_schema()
        snapshot_id = store.write_snapshot(
            taken_at_ms=taken_at_ms,
            finished_at_ms=finished_at_ms,
            mode=mode,
            parquet_path=str(parquet_path),
            is_valid=is_valid,
            market_rows=target_markets,
            issues=issues,
            event_rows=event_rows,
            event_tag_rows=event_tag_rows,
        )

    # ── 7.5. Supabase mirror (D-02 dashboard) — fail-soft post-write ─────────
    # SQLite + Parquet are the source of truth (D-12 amendment). Mirror failure
    # → DEGRADED (not FAILED). Does NOT increment scheduler's failure_counter.
    #
    # F-05 (Plan 02-08): pre-empt the whole mirror block when the snapshot
    # is invalid (e.g. 0-market case caused by an API_UNREACHABLE on /markets).
    # The validator marks is_valid=False there; mirroring such a degenerate
    # row would land a status="failed" / market_count=0 row in Supabase that
    # is_valid_overall already says we shouldn't trust. Fail-soft policy says
    # "skip, don't corrupt".
    mirror = None  # type: ignore[assignment]
    if settings.supabase_mirror_enabled and not is_valid:
        logger.info(
            f"step 7.5: skip Supabase mirror — snapshot is_valid=False "
            f"(snapshot_id={snapshot_id}, status={status.value}); F-05 guard"
        )
    elif settings.supabase_mirror_enabled:
        from polyarb.storage.supabase_mirror import SupabaseMirror, narrow_market_row
        try:
            mirror = SupabaseMirror(
                settings.supabase_url,
                settings.supabase_service_key.get_secret_value(),
            )
            narrow_rows = [narrow_market_row(m, snapshot_id) for m in target_markets]
            snapshot_meta = {
                "id": snapshot_id,
                "taken_at_ms": taken_at_ms,
                "finished_at_ms": finished_at_ms,
                "mode": mode,
                "status": status.value,
                "market_count": len(target_markets),
                "parquet_url": None,  # Updated in step 7.6 if R2 upload succeeds
            }
            ok = mirror.push_snapshot(snapshot_id, snapshot_meta, narrow_rows)
            if ok:
                # Record successful mirror timestamp in SQLite (non-critical; ignore failure)
                try:
                    store.update_snapshot_mirror_fields(
                        snapshot_id,
                        supabase_mirror_at_ms=int(time.time() * 1000),
                    )
                except Exception:  # noqa: BLE001
                    pass
            else:
                issues.append(
                    Issue(
                        layer=4,
                        category=Category.UNKNOWN,
                        market_id=None,
                        detail=f"Supabase mirror push returned False (fail-soft, snapshot_id={snapshot_id})",
                    )
                )
        except Exception as e:  # noqa: BLE001
            logger.error(f"Supabase mirror init failed: {e!r}")
            issues.append(
                Issue(
                    layer=4,
                    category=Category.UNKNOWN,
                    market_id=None,
                    detail=f"Supabase mirror init failed: {str(e)[:200]}",
                )
            )
            mirror = None  # type: ignore[assignment]

    # ── 7.6. R2 parquet archive (D-03) — fail-soft post-write ────────────────
    # Upload the already-written parquet to Cloudflare R2. Failure → DEGRADED.
    if settings.r2_enabled:
        from polyarb.storage.r2_sync import R2UploadError, compute_r2_key, upload_parquet_to_r2
        r2_url: str | None = None
        try:
            r2_key = compute_r2_key(taken_at_ms)
            r2_url = upload_parquet_to_r2(
                parquet_path=parquet_path,
                bucket=settings.r2_bucket,
                key=r2_key,
                endpoint=settings.r2_endpoint,
                access_key=settings.r2_access_key_id.get_secret_value(),
                secret_key=settings.r2_secret_access_key.get_secret_value(),
            )
            # Record R2 URL in SQLite (non-critical; ignore failure)
            try:
                store.update_snapshot_mirror_fields(snapshot_id, parquet_r2_url=r2_url)
            except Exception:  # noqa: BLE001
                pass
        except R2UploadError as e:
            logger.error(f"R2 upload failed: {e!r}")
            issues.append(
                Issue(
                    layer=4,
                    category=Category.UNKNOWN,
                    market_id=None,
                    detail=f"R2 upload failed: {str(e)[:200]}",
                )
            )
            r2_url = None

        # Update Supabase snapshots.parquet_url if both mirror and R2 succeeded
        if r2_url is not None and mirror is not None and settings.supabase_mirror_enabled:
            try:
                mirror.update_parquet_url(snapshot_id, r2_url)
            except Exception:  # noqa: BLE001
                logger.warning("update_parquet_url post-r2 failed; snapshots.parquet_url stays NULL")

    # ── Cache cleanup — MUST run unconditionally (after step 7.5 + 7.6) ──────
    # Even if mirror/R2 failed, the local write succeeded — clean up cache.
    # Cache cleanup happens ONLY after a successful SQLite commit. If step 7
    # failed mid-way, the cache is left intact so the next run can resume.
    if cache is not None:
        cache.cleanup()

    logger.info(
        f"Snapshot complete in {_format_elapsed(time.monotonic() - overall_t0)} "
        f"(snapshot_id={snapshot_id})"
    )

    # Aggregate issues by category for the summary line.
    cat_counts: dict[str, int] = {}
    for i in issues:
        cat_counts[i.category.value] = cat_counts.get(i.category.value, 0) + 1

    return SnapshotResult(
        snapshot_id=snapshot_id,
        market_count=len(target_markets),  # what got persisted, not full normalize count
        is_valid=is_valid,
        status=status.value,
        mode=mode,
        issue_count=len(issues),
        issue_categories=cat_counts,
        parquet_path=parquet_path,
        taken_at_ms=taken_at_ms,
        finished_at_ms=finished_at_ms,
    )
