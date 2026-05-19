"""Validator layers — Layer 1 (count), Layer 2 (fields), Layer 4 (cross-source).

Phase 1 validity policy (resolved Q5 in CONTEXT.md):
  - Layer 1 STRICT: any count mismatch flips is_valid=false.
  - Layer 2/4: record-only — issues are persisted but is_valid stays true.
  Revisit after Phase 3 collects evidence on issue rates and false-positive cost.

Layer 4 ghost-book defense addresses Polymarket issue #180 (RESEARCH.md Pitfall 1):
when /book reports a top-of-book like ask=0.99/bid=0.01 (no real liquidity) but
/price (taker quote) returns ~0.55, the order book is "ghost" / stale and a naïve
arbitrage signal would mis-fire. We surface this divergence as a categorized Issue
so downstream code can skip the market instead of trading a phantom edge.

F-1 SECURITY (SECURITY-REVIEW.md): every coercion of an attacker-controlled CLOB
field (price/size in book payloads) is wrapped via `_safe_float` — a ValueError /
TypeError must not crash the validator. Unparseable books are flagged as a
Category.UNKNOWN Layer 4 issue with the raw payload truncated to 500 bytes.

F-5 SECURITY: `detail` is capped at 200 chars and `raw_payload` at 1024 bytes.
A market with a 10MB `question` field would otherwise inflate validation_issues
without bound.

Pure functions only — no logging, no IO, no async.
"""

from __future__ import annotations

import json
from typing import Any

from polyarb.validator.category import Category, Issue, SnapshotStatus

REQUIRED_FIELDS: tuple[str, ...] = (
    "market_id",
    "condition_id",
    "yes_token_id",
    "no_token_id",
    "mid_price",
    "liquidity_usd",
    "end_time_ms",
)

# Ghost-book detection thresholds (Pitfall 1)
GHOST_BOOK_ASK_THRESHOLD: float = 0.98
GHOST_BOOK_BID_THRESHOLD: float = 0.02
GHOST_BOOK_PRICE_DIVERGENCE: float = 0.05

# Layer 2 categorization heuristics
ZOMBIE_LIQUIDITY_USD: float = 10.0
RESOLVING_WINDOW_MS: int = 24 * 60 * 60 * 1000  # 24 hours

# F-5 truncation caps
_DETAIL_MAX_CHARS: int = 200
_RAW_PAYLOAD_MAX_BYTES: int = 1024
_BOOK_PAYLOAD_MAX_BYTES: int = 500  # smaller cap for ghost-book payloads

# Layer 1 count mismatch tolerance (ratio). Observed in production:
# 200/58835 = 0.34%. Below this, the discrepancy is DEGRADED not FAILED.
L1_COUNT_TOLERANCE: float = 0.01  # 1%


def _safe_float(v: Any) -> float | None:
    """Coerce to float, return None on any coercion failure (F-1).

    Catches KeyError, TypeError, ValueError. Attacker-controlled CLOB payloads
    must not crash the validator with raw exceptions.
    """
    try:
        return float(v)
    except (KeyError, TypeError, ValueError):
        return None


def _is_missing(v: Any) -> bool:
    """Treat None / "" / [] / {} as missing for Layer 2 field-presence check."""
    if v is None:
        return True
    if isinstance(v, (str, list, tuple, dict, set)) and len(v) == 0:
        return True
    return False


def layer1_count(reported_total: int, fetched_count: int) -> list[Issue]:
    """Layer 1: strict equality between Gamma's reported active count and fetched count.

    Any mismatch (over OR under) flips is_valid=False — this is the only layer
    that drives is_valid in Phase 1.
    """
    if reported_total != fetched_count:
        return [
            Issue(
                layer=1,
                category=Category.API_JITTER,
                market_id=None,
                detail=(
                    f"Gamma reported {reported_total} active markets, fetched {fetched_count}"
                )[:_DETAIL_MAX_CHARS],
            )
        ]
    return []


def layer2_fields(markets: list[dict], *, now_ms: int) -> list[Issue]:
    """Layer 2: per-market required-field presence check.

    SIDE EFFECT: mutates each market with `incomplete=True` when fields are
    missing (RESEARCH.md Pattern 5 — mark, don't drop). Caller must be aware
    that the input list is mutated in place.

    Categorization heuristic (best-effort, refinable in later phases):
      - end_time_ms within 24h         → RESOLVING (market is being resolved)
      - liquidity_usd < $10            → ZOMBIE_MARKET
      - otherwise                      → UNKNOWN (system debt — converge to specifics)
    """
    issues: list[Issue] = []
    for m in markets:
        missing = [k for k in REQUIRED_FIELDS if _is_missing(m.get(k))]
        if not missing:
            continue

        end_time_ms = m.get("end_time_ms")
        liquidity_usd = m.get("liquidity_usd")

        category: Category
        if (
            end_time_ms is not None
            and isinstance(end_time_ms, (int, float))
            and 0 < (end_time_ms - now_ms) < RESOLVING_WINDOW_MS
        ):
            category = Category.RESOLVING
        elif (
            liquidity_usd is not None
            and isinstance(liquidity_usd, (int, float))
            and liquidity_usd < ZOMBIE_LIQUIDITY_USD
        ):
            category = Category.ZOMBIE_MARKET
        else:
            category = Category.UNKNOWN

        # F-5: truncate detail and raw_payload to bound DB row size.
        detail = f"missing: {missing}"[:_DETAIL_MAX_CHARS]
        raw_payload = json.dumps(
            {k: m.get(k) for k in REQUIRED_FIELDS},
            default=str,
        )[:_RAW_PAYLOAD_MAX_BYTES]

        issues.append(
            Issue(
                layer=2,
                category=category,
                market_id=m.get("market_id"),
                detail=detail,
                raw_payload=raw_payload,
            )
        )
        # Pattern 5: mark the row as incomplete; downstream pipelines can decide
        # whether to drop, surface, or analyze separately.
        m["incomplete"] = True

    return issues


def layer4_cross_source(
    markets: list[dict],
    books_by_token: dict[str, dict],
    prices_by_token: dict[str, Any],
) -> list[Issue]:
    """Layer 4: cross-source check between /book and /price (Pitfall 1).

    For every (market, token) pair (yes + no):
      1. CLOB_MISSING: token has no entry in books_by_token.
      2. UNKNOWN: book exists but top-of-book prices fail to parse as float (F-1).
      3. GHOST_BOOK: book reports ask>0.98 AND bid<0.02 (looks dead) BUT /price
         disagrees with book ask by more than 0.05 — book is stale / fake.
    """
    issues: list[Issue] = []
    for m in markets:
        market_id = m.get("market_id")
        for token_field in ("yes_token_id", "no_token_id"):
            tid = m.get(token_field)
            if not tid:
                continue

            if tid not in books_by_token:
                issues.append(
                    Issue(
                        layer=4,
                        category=Category.CLOB_MISSING,
                        market_id=market_id,
                        detail=(f"CLOB has no book for {token_field}={tid}")[
                            :_DETAIL_MAX_CHARS
                        ],
                    )
                )
                continue

            book = books_by_token[tid] or {}
            # F-1: normalize asks/bids to list — guards against non-list types
            # (dict, str, None) that would raise TypeError/KeyError on indexing.
            _raw_asks = book.get("asks")
            _raw_bids = book.get("bids")
            asks = _raw_asks if isinstance(_raw_asks, (list, tuple)) else []
            bids = _raw_bids if isinstance(_raw_bids, (list, tuple)) else []

            # F-1: defend against attacker-controlled non-numeric / missing fields.
            top_ask_price: float | None = None
            top_bid_price: float | None = None
            unparseable = False

            if asks:
                try:
                    # F-1: asks may be a non-list (dict, str, None) — guard with
                    # isinstance first; also catch KeyError for dict subscript.
                    first_ask = asks[0] if isinstance(asks, (list, tuple)) else None
                    raw = first_ask.get("price") if isinstance(first_ask, dict) else None
                except (AttributeError, IndexError, KeyError, TypeError):
                    raw = None
                top_ask_price = _safe_float(raw)
                if top_ask_price is None:
                    unparseable = True

            if bids and not unparseable:
                try:
                    first_bid = bids[0] if isinstance(bids, (list, tuple)) else None
                    raw = first_bid.get("price") if isinstance(first_bid, dict) else None
                except (AttributeError, IndexError, KeyError, TypeError):
                    raw = None
                top_bid_price = _safe_float(raw)
                if top_bid_price is None and bids:
                    unparseable = True

            if unparseable:
                issues.append(
                    Issue(
                        layer=4,
                        category=Category.UNKNOWN,
                        market_id=market_id,
                        detail=(f"unparseable book for {token_field}={tid}")[
                            :_DETAIL_MAX_CHARS
                        ],
                        raw_payload=json.dumps(book, default=str)[
                            :_BOOK_PAYLOAD_MAX_BYTES
                        ],
                    )
                )
                continue

            # Ghost-book detection requires BOTH bid and ask to be present and parseable.
            if top_ask_price is None or top_bid_price is None:
                continue
            if not (
                top_ask_price > GHOST_BOOK_ASK_THRESHOLD
                and top_bid_price < GHOST_BOOK_BID_THRESHOLD
            ):
                continue

            # Compare against /price reference (taker quote).
            ref = prices_by_token.get(tid)
            if ref is None:
                # No reference — cannot adjudicate. Skip silently (not an issue).
                continue
            if isinstance(ref, dict):
                ref_raw = ref.get("buy")
            else:
                ref_raw = ref

            ref_val = _safe_float(ref_raw)
            if ref_val is None:
                continue

            if abs(ref_val - top_ask_price) > GHOST_BOOK_PRICE_DIVERGENCE:
                issues.append(
                    Issue(
                        layer=4,
                        category=Category.GHOST_BOOK,
                        market_id=market_id,
                        detail=(
                            f"book bid={top_bid_price}/ask={top_ask_price} but "
                            f"/price={ref_val}"
                        )[:_DETAIL_MAX_CHARS],
                    )
                )

    return issues


def determine_snapshot_status(issues: list[Issue]) -> SnapshotStatus:
    """Return snapshot health based on Layer 1 issue severity.

    - API_UNREACHABLE at Layer 1 → FAILED (core data source down)
    - Count mismatch > L1_COUNT_TOLERANCE → FAILED
    - Count mismatch ≤ L1_COUNT_TOLERANCE → DEGRADED
    - No Layer 1 issues → OK

    Layer 2/4 issues are recorded but never affect the status.
    """
    l1_issues = [i for i in issues if i.layer == 1]
    if not l1_issues:
        return SnapshotStatus.OK

    for i in l1_issues:
        if i.category == Category.API_UNREACHABLE:
            return SnapshotStatus.FAILED

    # Count jitter — parse reported vs fetched from the detail string
    for i in l1_issues:
        if i.category == Category.API_JITTER:
            # detail format: "Gamma reported N active markets, fetched M"
            import re

            m = re.search(r"reported (\d+).*?fetched (\d+)", i.detail)
            if m:
                reported = int(m.group(1))
                fetched = int(m.group(2))
                if reported > 0:
                    discrepancy = abs(reported - fetched) / reported
                    if discrepancy > L1_COUNT_TOLERANCE:
                        return SnapshotStatus.FAILED
            # If we can't parse the counts, default to DEGRADED
            return SnapshotStatus.DEGRADED

    # Unknown L1 category — conservative fallback
    return SnapshotStatus.DEGRADED


def is_valid_overall(issues: list[Issue]) -> bool:
    """Return True if snapshot is usable (OK or DEGRADED).

    Only FAILED status flips is_valid to False. DEGRADED (minor count jitter
    ≤ 1%) still returns True — the data is usable but should be noted.
    """
    return determine_snapshot_status(issues) != SnapshotStatus.FAILED
