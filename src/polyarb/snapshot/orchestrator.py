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

import httpx
import sentry_sdk
from loguru import logger
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.clients.gamma_client import GammaClient, PaginationCoverage
from polyarb.config import Settings
from polyarb.events.bus import publish_snapshot_complete
from polyarb.perception.market_truth import (
    EventMember,
    GroupTruth,
    SourceCoverage,
    market_truth_mismatch_reason,
)
from polyarb.snapshot.cache import ChunkCache
from polyarb.snapshot.normalizer import normalize_events, normalize_market
from polyarb.storage.parquet_writer import compute_snapshot_path, write_parquet_streaming
from polyarb.storage.sqlite_store import SQLiteStore
from polyarb.validator.category import Category, Issue, SnapshotStatus
from polyarb.validator.layers import (
    determine_snapshot_status,
    is_valid_overall,
    layer1_count,
    layer2_fields,
    layer4_cross_source,
)


def _is_dns_jitter(exc: BaseException) -> bool:
    """Match the specific DNS-failure exception shapes seen in Fly machine production.

    Phase 03.1 D-01 modify A — Sentry issue 121111789 evidence (6 days, 3 occurrences):
      - "[Errno -5] No address associated with hostname"   (EAI_NODATA)
      - "[Errno -3] Temporary failure in name resolution"  (EAI_AGAIN)

    Strictly DNS-class: refuses to retry other ConnectErrors (connection
    refused, host unreachable) — those signal real upstream outages and the
    existing fail-soft Issue(API_UNREACHABLE) path must remain intact
    (chain-truth discipline; ref feedback_code-vs-chain-truth-2026-05).
    """
    if not isinstance(exc, httpx.ConnectError):
        return False
    s = str(exc)
    return (
        "[Errno -5]" in s
        or "[Errno -3]" in s
        or "EAI_AGAIN" in s
        or "EAI_NODATA" in s
        or "Name or service not known" in s
        or "Temporary failure in name resolution" in s
    )


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


def _derive_notes_from_issues(issues: list[Issue]) -> str | None:
    """Phase 03.1 Plan 02 GAP-103 — pull a one-line fail-reason for snapshots.notes.

    Operators need to know WHY a snapshot failed without joining the
    validation_issues table. Strategy: collect API_UNREACHABLE details (truncated
    to 80 chars each), semicolon-join, cap total at 200 chars.

    Returns None when no API_UNREACHABLE issues are present — clean / validator-
    only-noise snapshots keep notes=NULL (operators want fail reasons, not L2
    validation findings like zombie_market or ghost_book).

    Format chosen so dashboards can `SELECT substr(notes, 1, 40), count(*) FROM
    snapshots GROUP BY 1` to tally failure modes.
    """
    reasons: list[str] = []
    for issue in issues:
        if issue.category != Category.API_UNREACHABLE:
            continue
        detail = issue.detail or f"L{issue.layer}: unknown"
        reasons.append(detail[:80])
    if not reasons:
        return None
    joined = "; ".join(reasons)
    return joined[:200]


@dataclass
class SnapshotResult:
    """Return value of ``run_snapshot`` — what the CLI prints + what tests assert on."""

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


def _include_in_snapshot(mode: str, market: dict, threshold: float) -> bool:
    """Keep liquid markets plus every neg-risk sibling needed by M2."""
    return (
        mode == "full"
        or bool(market.get("neg_risk_market_id"))
        or (market.get("liquidity_usd") or 0) > threshold
    )


def _reconcile_market_truth(
    *,
    observed_market_ids: set[str],
    normalized_market_rows: list[dict],
    market_to_event_map: dict[str, str],
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
) -> str | None:
    """Reconcile full Gamma identities before any subset publication claim."""
    incomplete_truth = next(
        (truth for truth in group_truths if truth.quality == "incomplete-source"),
        None,
    )
    if incomplete_truth is not None:
        reason = incomplete_truth.reason or "unspecified"
        return f"group-incomplete-source:{incomplete_truth.group_id}:{reason}"[:160]

    truth_keys: set[tuple[str, str]] = set()
    group_ids: set[str] = set()
    for truth in group_truths:
        key = (truth.event_id, truth.group_id)
        if key in truth_keys or truth.group_id in group_ids:
            return f"duplicate-group-identity:{truth.event_id}/{truth.group_id}"[:160]
        truth_keys.add(key)
        group_ids.add(truth.group_id)

    seen_member_ids: set[str] = set()
    for member in event_members:
        if member.market_id in seen_member_ids:
            return f"duplicate-member-identity:{member.market_id}"[:160]
        seen_member_ids.add(member.market_id)
        if (member.event_id, member.group_id) not in truth_keys:
            return f"member-without-group-truth:{member.market_id}"[:160]
        mapped_event = market_to_event_map.get(member.market_id)
        if mapped_event != member.event_id:
            return (
                f"event-member-identity-conflict:{member.market_id}:"
                f"{member.event_id}!={mapped_event}"
            )[:160]

    missing_members = sorted(seen_member_ids - observed_market_ids)
    if missing_members:
        return f"event-member-missing-market:{','.join(missing_members[:5])}"[:160]

    orphan_markets = sorted(observed_market_ids - set(market_to_event_map))
    if orphan_markets:
        return f"orphan-market-without-event:{','.join(orphan_markets[:5])}"[:160]

    missing_mapped_markets = sorted(set(market_to_event_map) - observed_market_ids)
    if missing_mapped_markets:
        return f"event-map-missing-market:{','.join(missing_mapped_markets[:5])}"[:160]
    semantic_reason = market_truth_mismatch_reason(
        event_members,
        group_truths,
        normalized_market_rows,
    )
    if semantic_reason is not None:
        return semantic_reason[:160]
    return None


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
    target_markets: list[dict] = []
    seen_ids: set[str] = set()
    raw_market_count = 0
    normalized_count = 0
    dedup_count = 0
    event_rows: list[dict] = []
    event_tag_rows: list[dict] = []
    event_members: list[EventMember] = []
    group_truths: list[GroupTruth] = []
    market_to_event_map: dict[str, str] = {}
    events_coverage = PaginationCoverage(source="events")
    markets_coverage = PaginationCoverage(source="markets")
    event_failure_reason: str | None = None
    market_failure_reason: str | None = None
    threshold = settings.liquidity_threshold_usd
    PROGRESS_EVERY = 5000  # log every N streamed markets so long fetches stay observable

    logger.info(
        f"snapshot starting — mode={mode}, cache={'on' if use_cache else 'off'}, "
        f"taken_at_ms={taken_at_ms}"
    )

    overall_t0 = time.monotonic()

    # ── Phases 1+2 combined: one GammaClient session, events materialized,
    # then markets STREAMED (D-23). Single `async with` so HTTP/2 keepalive
    # is shared across /events + /markets and shutdown is clean.
    async with GammaClient(settings) as gamma:
        # ── Phase 1: events (fully materialized — Decision A) ─────────────
        with _phase("1/7: Gamma /events fetch + normalize"):
            try:
                raw_events = [
                    event async for event in gamma.iter_active_events(events_coverage)
                ]
                logger.info(f"Gamma: fetched {len(raw_events)} active events")
                (
                    event_rows,
                    event_tag_rows,
                    market_to_event_map,
                    event_members,
                    group_truths,
                ) = normalize_events(raw_events)
                del raw_events  # free 10k+ raw Gamma event dicts immediately
                logger.info(
                    f"Events normalized: {len(event_rows)} events, "
                    f"{len(event_tag_rows)} event_tags, "
                    f"{len(market_to_event_map)} market→event mappings"
                )
            except Exception as e:  # noqa: BLE001 — categorize, do NOT propagate
                event_failure_reason = str(e)[:200]
                logger.error(f"Gamma /events fetch failed: {e!r}")
                issues.append(
                    Issue(
                        layer=1,
                        category=Category.API_UNREACHABLE,
                        market_id=None,
                        detail=f"Gamma /events unreachable: {str(e)[:200]}",
                    )
                )

        # ── Phase 2: stream /markets — normalize + dedupe + mode-filter ──
        # The 20k raw Gamma /markets list NEVER materializes — each `raw`
        # dict is normalized, dedup-checked, mode-filtered, and either
        # appended to `target_markets` or dropped. Non-target markets go
        # out of scope at the next iteration → GC eligible.
        #
        # Phase 03.1-04 D-01 modify A: wrap the stream-START in tenacity
        # AsyncRetrying that fires ONLY for DNS-class ConnectError (EAI_NODATA
        # / EAI_AGAIN). first_frame_seen sentinel ensures middle-of-stream
        # exceptions are NOT retried (a partial stream consumed N markets ≠
        # idempotent retry boundary). Stops at 3 attempts, exponential wait
        # 1..5s. Re-raises last exception → existing fail-soft except clause
        # below appends API_UNREACHABLE (chain-truth preserved).
        with _phase("2/7: Stream /markets — normalize + dedupe + filter"):
            first_frame_seen = False
            authoritative_member_ids = {
                member.market_id for member in event_members
            }
            try:
                async for retry_state in AsyncRetrying(
                    retry=retry_if_exception(lambda e: _is_dns_jitter(e) and not first_frame_seen),
                    stop=stop_after_attempt(3),
                    wait=wait_exponential(multiplier=1, min=1, max=5),
                    reraise=True,
                ):
                    with retry_state:
                        async for raw in gamma.iter_active_markets(markets_coverage):
                            if not first_frame_seen:
                                first_frame_seen = True
                            raw_market_count += 1
                            normalized = normalize_market(raw, market_to_event_map)
                            if normalized is None:
                                continue
                            mid = normalized.get("market_id")
                            if mid is None:
                                continue
                            if mid in seen_ids:
                                dedup_count += 1
                                continue
                            seen_ids.add(mid)
                            normalized_count += 1

                            # Mode filter (replaces the old phase-3 block).
                            if (
                                mid in authoritative_member_ids
                                or _include_in_snapshot(mode, normalized, threshold)
                            ):
                                target_markets.append(normalized)
                            # Non-target markets: dropped — no buffer, no reference held.

                            if normalized_count % PROGRESS_EVERY == 0:
                                logger.info(
                                    f"streaming {normalized_count} markets normalized, "
                                    f"{len(target_markets)} target so far"
                                )
            except Exception as e:  # noqa: BLE001 — categorize, do NOT propagate
                market_failure_reason = str(e)[:200]
                logger.error(f"Gamma /markets stream failed: {e!r}")
                issues.append(
                    Issue(
                        layer=1,
                        category=Category.API_UNREACHABLE,
                        market_id=None,
                        detail=f"Gamma unreachable: {str(e)[:200]}",
                    )
                )

    # GammaClient closed (exited async-with). httpx AsyncClient fully cleaned
    # up before the CLOB phase starts.

    unique_market_count = raw_market_count - dedup_count
    gamma_count_reported = unique_market_count if raw_market_count > 0 else None
    if dedup_count > 0:
        logger.info(f"Deduped {dedup_count} markets by market_id (Gamma pagination overlap)")
    logger.info(
        f"Streamed {normalized_count}/{raw_market_count} normalized; "
        f"{len(target_markets)} target after mode-filter (mode={mode})"
    )
    reconciliation_reason: str | None = None
    if events_coverage.result.completed and markets_coverage.result.completed:
        reconciliation_reason = _reconcile_market_truth(
            observed_market_ids=seen_ids,
            normalized_market_rows=target_markets,
            market_to_event_map=market_to_event_map,
            event_members=event_members,
            group_truths=group_truths,
        )
        if reconciliation_reason is not None:
            issues.append(
                Issue(
                    layer=1,
                    category=Category.API_UNREACHABLE,
                    market_id=None,
                    detail=(
                        "Gamma event/market reconciliation incomplete: "
                        f"{reconciliation_reason}"
                    )[:200],
                )
            )

    # ── Phase 3: token list extraction (was inlined into old phase 3) ────
    with _phase("3/7: Build token list"):
        token_ids: list[str] = []
        for m in target_markets:
            for k in ("yes_token_id", "no_token_id"):
                tid = m.get(k)
                if tid:
                    token_ids.append(tid)
        logger.info(
            f"Mode={mode}: {len(target_markets)} target markets, "
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
            # F-1 SECURITY: book fields may be attacker-controlled non-list types.
            # Normalize asks/bids to list before any indexing — guards against
            # dict/str/None values that would raise TypeError/KeyError on [0].
            _raw_asks = book.get("asks")
            _raw_bids = book.get("bids")
            asks = _raw_asks if isinstance(_raw_asks, (list, tuple)) else []
            bids = _raw_bids if isinstance(_raw_bids, (list, tuple)) else []

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
            issues.extend(layer1_count(gamma_count_reported, normalized_count))

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
        source_complete = (
            events_coverage.result.completed and markets_coverage.result.completed
            and reconciliation_reason is None
        )
        publish_markets = source_complete and is_valid and not any(
            issue.category == Category.API_UNREACHABLE for issue in issues
        )
        if not publish_markets:
            status = SnapshotStatus.FAILED
            is_valid = False
        logger.info(
            f"Validated: status={status.value}, is_valid={is_valid}, "
            f"publish_markets={publish_markets}, {len(issues)} total issues "
            f"({sum(1 for i in issues if i.layer == 1)} L1, "
            f"{sum(1 for i in issues if i.layer == 2)} L2, "
            f"{sum(1 for i in issues if i.layer == 4)} L4)"
        )

    # ── 7. Persist (Parquet atomic FIRST, then SQLite single-tx) ──────────────
    finished_at_ms = int(time.time() * 1000)
    parquet_path = compute_snapshot_path(settings.parquet_root, taken_at_ms)
    with _phase("7/7: Persist (Parquet then SQLite)"):
        # Plan 02-09 (D-23): streaming writes. Parquet via ParquetWriter chunked
        # write; SQLite via batched executemany in a single BEGIN IMMEDIATE
        # transaction. Both consume `target_markets` (already post-filter, ≤8k
        # rows in subset mode at $1k threshold). The architectural win is in
        # phase 2 above — the 20k raw Gamma list never materializes.

        def _parquet_row_iter():
            """Generator: stamp snapshot metadata on each target market dict."""
            for m in target_markets:
                row = dict(m)
                row["snapshot_taken_at_ms"] = taken_at_ms
                row["snapshot_id"] = 0  # SQLite assigns the real id
                row.setdefault("fetched_at_ms", clob_done_ms)
                yield row

        write_parquet_streaming(_parquet_row_iter(), parquet_path, batch_size=500)

        # Phase 1.1 Amendment 01: stamp events with finished_at_ms (NOT
        # clob_done_ms — events are fetched by Gamma in phase 1, not CLOB).
        for ev in event_rows:
            if ev.get("fetched_at_ms") is None:
                ev["fetched_at_ms"] = finished_at_ms

        store = SQLiteStore(settings.db_path)
        store.init_schema()
        if source_complete:
            source_coverage = SourceCoverage.complete(
                markets_coverage.result.items_yielded,
                events_coverage.result.items_yielded,
            )
        elif reconciliation_reason is not None:
            source_coverage = SourceCoverage.incomplete(
                "events",
                markets_coverage.result.items_yielded,
                events_coverage.result.items_yielded,
                reconciliation_reason,
            )
        elif not events_coverage.result.completed:
            source_coverage = SourceCoverage.incomplete(
                "events",
                markets_coverage.result.items_yielded,
                events_coverage.result.items_yielded,
                event_failure_reason or "event-pagination-incomplete",
            )
        else:
            source_coverage = SourceCoverage.incomplete(
                "markets",
                markets_coverage.result.items_yielded,
                events_coverage.result.items_yielded,
                market_failure_reason or "market-pagination-incomplete",
            )
        snapshot_id, market_count = store.write_snapshot_streaming(
            taken_at_ms=taken_at_ms,
            finished_at_ms=finished_at_ms,
            mode=mode,
            parquet_path=str(parquet_path),
            is_valid=is_valid,
            market_rows=target_markets,
            issues=issues,
            source_coverage=source_coverage,
            event_members=event_members,
            group_truths=group_truths,
            publish_markets=publish_markets,
            notes=_derive_notes_from_issues(issues),  # Plan 03.1-02 GAP-103
            event_rows=event_rows,
            event_tag_rows=event_tag_rows,
            batch_size=500,
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
    if settings.supabase_mirror_enabled and not publish_markets:
        logger.info(
            f"step 7.5: skip Supabase mirror — market truth was not published "
            f"(snapshot_id={snapshot_id}, status={status.value})"
        )
    elif settings.supabase_mirror_enabled and not is_valid:
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
                        detail=(
                            "Supabase mirror push returned False "
                            f"(fail-soft, snapshot_id={snapshot_id})"
                        ),
                    )
                )
        except Exception as e:  # noqa: BLE001
            logger.error(f"Supabase mirror init failed: {e!r}")
            # Plan 05: Sentry breadcrumb (warning level) — adds context to
            # the NEXT real Sentry event without opening a new issue. Mirror
            # failures are fail-soft; we don't want them spamming Sentry.
            sentry_sdk.add_breadcrumb(
                category="storage",
                message=f"supabase_mirror_failed snapshot_id={snapshot_id}",
                level="warning",
                data={"error": str(e)[:200]},
            )
            issues.append(
                Issue(
                    layer=4,
                    category=Category.UNKNOWN,
                    market_id=None,
                    detail=f"Supabase mirror init failed: {str(e)[:200]}",
                )
            )
            mirror = None  # type: ignore[assignment]
    else:
        # D-01 (Phase 02.1, BUG-7): audit log + breadcrumb for config-disabled skip.
        # Previously this branch was completely silent — daemon log had nothing,
        # Sentry events had no breadcrumb context. The 2026-05 chaos Inj 3
        # (撤 POLYARB_SUPABASE_SERVICE_KEY → pydantic flips mirror_enabled=False)
        # surfaced this as Bug #7: fail-soft path collapsed to a black hole.
        #
        # D-12 invariant: fail-soft contract unchanged — snapshot still completes.
        logger.info(
            f"step 7.5: mirror disabled — reason=config-disabled "
            f"(snapshot_id={snapshot_id}). "
            "Supabase dashboard will not update until mirror is re-enabled."
        )
        sentry_sdk.add_breadcrumb(
            category="mirror",
            level="info",
            message="mirror skipped: reason=config-disabled",
            data={"supabase_mirror_enabled": False, "snapshot_id": snapshot_id},
        )

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
            # Plan 05: Sentry breadcrumb (warning level) — captures storage
            # failure context for the next real Sentry event without
            # opening a separate issue (fail-soft path, don't pollute Sentry).
            sentry_sdk.add_breadcrumb(
                category="storage",
                message=f"r2_upload_failed snapshot_id={snapshot_id}",
                level="warning",
                data={"error": str(e)[:200]},
            )
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
                logger.warning(
                    "update_parquet_url post-r2 failed; snapshots.parquet_url stays NULL"
                )

    # ── 7.7. Event bus fan-out (Plan 03-05, D-05) — fail-soft post-write ─────
    # L1 → L2 cross-process NOTIFY so the L2 daemon can refresh its
    # candidate WS subscription set. Feature-flag `event_bus_enabled`
    # default FALSE per B1 spawn constraint — opt-in via Fly secret
    # `POLYARB_EVENT_BUS_ENABLED=1` ONLY after Plan 07 chaos PASS for
    # Inj L2-3. Wrapped in try/except so a NOTIFY failure NEVER blocks
    # snapshot completion (D-12 invariant). publish_snapshot_complete
    # itself is fail-soft, but we belt-and-suspender the import call too.
    if getattr(settings, "event_bus_enabled", False) and publish_markets:
        try:
            await publish_snapshot_complete(
                settings,
                snapshot_id=snapshot_id,
                taken_at_ms=taken_at_ms,
            )
        except Exception as e:  # noqa: BLE001
            logger.warning(f"event bus publish failed (fail-soft): {e!r}")
            sentry_sdk.add_breadcrumb(
                category="event-bus",
                level="warning",
                message=f"orchestrator step 7.7 publish failed: {snapshot_id}",
                data={"error": str(e)[:200]},
            )

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
