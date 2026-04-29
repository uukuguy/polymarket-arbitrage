"""Snapshot orchestrator — wires Gamma + CLOB + validator + storage together.

The 7-step pipeline (per RESEARCH.md "Atomic SQLite + Parquet 编排" spec):

    1. Gamma fetch       — full active-market list
    2. Normalize         — Gamma raw → storage row contract (drop unrecoverable)
    3. Mode filter       — subset (liquidity>$1k) | full (all markets)
    4. CLOB batch fetch  — order books + buy/sell prices for the filtered tokens
    5. Stamp + attach    — fetched_at_ms (Pitfall 6) + best_bid/best_ask top-of-book
    6. Validate          — Layer 1 count, Layer 2 fields, Layer 4 cross-source
    7. Persist           — Parquet atomic write FIRST, then SQLite single-tx write

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
from dataclasses import dataclass
from pathlib import Path

from loguru import logger

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.clients.gamma_client import GammaClient
from polyarb.config import Settings
from polyarb.snapshot.normalizer import normalize_market
from polyarb.storage.parquet_writer import compute_snapshot_path, write_parquet_atomic
from polyarb.storage.sqlite_store import SQLiteStore
from polyarb.validator.category import Category, Issue
from polyarb.validator.layers import (
    is_valid_overall,
    layer1_count,
    layer2_fields,
    layer4_cross_source,
)


@dataclass
class SnapshotResult:
    """Return value of ``run_snapshot`` — what the CLI prints + what tests assert on."""

    snapshot_id: int
    market_count: int
    is_valid: bool
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
    """
    if mode not in ("subset", "full"):
        raise ValueError(f"invalid mode: {mode!r} (must be 'subset' or 'full')")

    taken_at_ms = now_ms if now_ms is not None else int(time.time() * 1000)
    issues: list[Issue] = []
    gamma_count_reported: int | None = None
    raw_markets: list[dict] = []

    # ── 1. Gamma fetch ────────────────────────────────────────────────────────
    async with GammaClient(settings) as gamma:
        try:
            raw_markets = await gamma.fetch_all_active_markets()
            gamma_count_reported = len(raw_markets)
            logger.info(f"Gamma: fetched {gamma_count_reported} active markets")
        except Exception as e:  # noqa: BLE001 — categorize, do NOT propagate
            logger.error(f"Gamma fetch failed: {e!r}")
            # F-5: cap exception detail to 200 chars (HTML 4xx body could be huge).
            issues.append(
                Issue(
                    layer=1,
                    category=Category.API_UNREACHABLE,
                    market_id=None,
                    detail=f"Gamma unreachable: {str(e)[:200]}",
                )
            )
            raw_markets = []

    # ── 2. Normalize (drop unrecoverable rows) ────────────────────────────────
    markets: list[dict] = [m for m in (normalize_market(r) for r in raw_markets) if m is not None]
    logger.info(f"Normalized: {len(markets)}/{len(raw_markets)} rows kept")

    # ── 3. Mode filter → token list ───────────────────────────────────────────
    if mode == "subset":
        target_markets = [
            m for m in markets if (m.get("liquidity_usd") or 0) > settings.liquidity_threshold_usd
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
    books_by_token: dict[str, dict] = {}
    prices_buy: dict = {}
    prices_sell: dict = {}
    clob = ClobReaderClient(settings)
    try:
        books = await clob.get_books(token_ids)
        prices = await clob.get_prices_buy_sell(token_ids)
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
    clob_done_ms = int(time.time() * 1000)
    for m in markets:
        # Phase-1 simplification: stamp ALL normalized markets even those
        # filtered out of the subset (documented in SUMMARY known limitations).
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
    if gamma_count_reported is not None:
        # Layer 1 compares Gamma's reported active count vs how many we kept
        # post-normalize. A diff means either a bug in normalize OR API jitter.
        issues.extend(layer1_count(gamma_count_reported, len(markets)))

    issues.extend(layer2_fields(markets, now_ms=taken_at_ms))

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
    issues.extend(layer4_cross_source(markets, books_by_token, prices_combined))

    is_valid = is_valid_overall(issues)
    logger.info(
        f"Validated: is_valid={is_valid}, {len(issues)} total issues "
        f"({sum(1 for i in issues if i.layer == 1)} L1, "
        f"{sum(1 for i in issues if i.layer == 2)} L2, "
        f"{sum(1 for i in issues if i.layer == 4)} L4)"
    )

    # ── 7. Persist (Parquet atomic FIRST, then SQLite single-tx) ──────────────
    finished_at_ms = int(time.time() * 1000)
    parquet_path = compute_snapshot_path(settings.parquet_root, taken_at_ms)

    # Build parquet rows — must match SNAPSHOT_SCHEMA (22 fields including
    # snapshot_taken_at_ms + snapshot_id parquet-only fields).
    parquet_rows: list[dict] = []
    for m in markets:
        row = dict(m)
        row["snapshot_taken_at_ms"] = taken_at_ms
        row["snapshot_id"] = 0  # placeholder — Parquet has no FK; SQLite assigns the real id
        # Ensure required-by-schema fields exist (defensive — normalizer guarantees these).
        row.setdefault("fetched_at_ms", clob_done_ms)
        parquet_rows.append(row)
    write_parquet_atomic(parquet_rows, parquet_path)

    store = SQLiteStore(settings.db_path)
    store.init_schema()
    snapshot_id = store.write_snapshot(
        taken_at_ms=taken_at_ms,
        finished_at_ms=finished_at_ms,
        mode=mode,
        parquet_path=str(parquet_path),
        is_valid=is_valid,
        market_rows=markets,
        issues=issues,
    )

    # Aggregate issues by category for the summary line.
    cat_counts: dict[str, int] = {}
    for i in issues:
        cat_counts[i.category.value] = cat_counts.get(i.category.value, 0) + 1

    return SnapshotResult(
        snapshot_id=snapshot_id,
        market_count=len(markets),
        is_valid=is_valid,
        mode=mode,
        issue_count=len(issues),
        issue_categories=cat_counts,
        parquet_path=parquet_path,
        taken_at_ms=taken_at_ms,
        finished_at_ms=finished_at_ms,
    )
