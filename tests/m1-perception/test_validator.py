"""Unit tests for polyarb.validator.layers.

17 tests covering:
- Layer 1 strict count equality (3 tests: match, undershoot, overshoot)
- Layer 2 field presence + categorization heuristic (5 tests: complete, zombie,
  resolving, unknown, mark-don't-drop)
- Layer 4 cross-source detection (6 tests: clob_missing, normal book, ghost
  book detected, no ghost when prices agree, no reference, two-token-per-market)
- is_valid_overall invariant (3 tests: empty, layer1 → False, layer 2/4-only → True)

Phase 1 policy (resolved Q5 / D-D1, D-D2): Layer 1 strict; Layer 2/4 record-only.
"""

from __future__ import annotations

# Belt-and-suspenders for F-3 path validator.
import os

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

from polyarb.validator.category import Category, Issue
from polyarb.validator.layers import (
    RESOLVING_WINDOW_MS,
    ZOMBIE_LIQUIDITY_USD,
    is_valid_overall,
    layer1_count,
    layer2_fields,
    layer4_cross_source,
)

# Fixed timestamp for deterministic Layer 2 tests: 2024-04-30T00:00:00Z
NOW_MS: int = 1_714_435_200_000


def make_market(market_id: str, **overrides) -> dict:
    base = dict(
        market_id=market_id,
        condition_id=f"c-{market_id}",
        slug="slug",
        question="Q?",
        yes_token_id="t-yes",
        no_token_id="t-no",
        mid_price=0.5,
        liquidity_usd=1000.0,
        end_time_ms=NOW_MS + 365 * 24 * 60 * 60 * 1000,  # 1y from NOW
    )
    base.update(overrides)
    return base


# ============================================================================
# Layer 1 — count equality
# ============================================================================


def test_layer1_match_no_issues() -> None:
    assert layer1_count(100, 100) == []


def test_layer1_mismatch_returns_api_jitter() -> None:
    issues = layer1_count(100, 99)
    assert len(issues) == 1
    iss = issues[0]
    assert iss.layer == 1
    assert iss.category == Category.API_JITTER
    assert iss.market_id is None
    assert "100" in iss.detail and "99" in iss.detail


def test_layer1_overshoot_also_flags() -> None:
    """Mismatch is symmetric: fetching MORE than reported is also a jitter signal."""
    issues = layer1_count(100, 101)
    assert len(issues) == 1
    assert issues[0].layer == 1


# ============================================================================
# Layer 2 — field presence + categorization
# ============================================================================


def test_layer2_complete_market_no_issue() -> None:
    m = make_market("m1")
    issues = layer2_fields([m], now_ms=NOW_MS)
    assert issues == []
    assert m.get("incomplete") is not True  # not mutated


def test_layer2_missing_field_marks_incomplete_and_categorizes_zombie() -> None:
    """liquidity < $10 → ZOMBIE_MARKET, and `incomplete` is set to True."""
    m = make_market(
        "z1",
        mid_price=None,  # missing field triggers Layer 2
        liquidity_usd=5.0,  # below ZOMBIE_LIQUIDITY_USD = 10
    )
    issues = layer2_fields([m], now_ms=NOW_MS)
    assert len(issues) == 1
    assert issues[0].category == Category.ZOMBIE_MARKET
    assert issues[0].layer == 2
    assert issues[0].market_id == "z1"
    assert m["incomplete"] is True  # SIDE EFFECT: Pattern 5 mark-don't-drop


def test_layer2_missing_field_categorizes_resolving_when_endtime_near() -> None:
    """end_time_ms within 24h → RESOLVING."""
    m = make_market(
        "r1",
        mid_price=None,
        end_time_ms=NOW_MS + 1_000_000,  # 1000 sec from NOW — well under 24h
        liquidity_usd=ZOMBIE_LIQUIDITY_USD * 100,  # NOT zombie-like
    )
    issues = layer2_fields([m], now_ms=NOW_MS)
    assert len(issues) == 1
    assert issues[0].category == Category.RESOLVING


def test_layer2_missing_field_categorizes_unknown_when_no_heuristic_matches() -> None:
    """Healthy-looking market (high liquidity, far end_time) but missing a field → UNKNOWN."""
    m = make_market(
        "u1",
        mid_price=None,
        liquidity_usd=10_000.0,  # NOT zombie
        end_time_ms=NOW_MS + 365 * 24 * 60 * 60 * 1000,  # 1y away — NOT resolving
    )
    issues = layer2_fields([m], now_ms=NOW_MS)
    assert len(issues) == 1
    assert issues[0].category == Category.UNKNOWN


def test_layer2_does_not_drop_market() -> None:
    """The market dict must remain in the input list with incomplete=True (mark, don't drop)."""
    m = make_market("k1", mid_price=None, liquidity_usd=5.0)
    markets = [m]
    layer2_fields(markets, now_ms=NOW_MS)
    assert markets == [m]  # same identity, same length
    assert markets[0]["incomplete"] is True


# ============================================================================
# Layer 4 — cross-source (CLOB book + /price)
# ============================================================================


def test_layer4_clob_missing_when_no_book() -> None:
    """Market with token but books_by_token={} → 1 CLOB_MISSING per missing token."""
    m = make_market("c1", yes_token_id="t1", no_token_id=None)
    issues = layer4_cross_source([m], books_by_token={}, prices_by_token={})
    assert len(issues) == 1
    assert issues[0].category == Category.CLOB_MISSING
    assert issues[0].layer == 4
    assert issues[0].market_id == "c1"
    assert "t1" in issues[0].detail


def test_layer4_no_issue_when_book_present_and_normal_prices() -> None:
    """Healthy book (no ghost-shape) → no Layer 4 issues."""
    m = make_market("ok1", yes_token_id="t1", no_token_id=None)
    books = {"t1": {"asks": [{"price": "0.55"}], "bids": [{"price": "0.45"}]}}
    prices = {"t1": {"buy": "0.55"}}
    issues = layer4_cross_source([m], books_by_token=books, prices_by_token=prices)
    assert issues == []


def test_layer4_ghost_book_detected() -> None:
    """Ghost book: bid=0.01/ask=0.99 but /price says 0.55 → flag GHOST_BOOK."""
    m = make_market("g1", yes_token_id="t1", no_token_id=None)
    books = {"t1": {"asks": [{"price": "0.99"}], "bids": [{"price": "0.01"}]}}
    prices = {"t1": {"buy": "0.55"}}
    issues = layer4_cross_source([m], books_by_token=books, prices_by_token=prices)
    assert len(issues) == 1
    assert issues[0].category == Category.GHOST_BOOK
    assert issues[0].market_id == "g1"
    assert "0.99" in issues[0].detail
    assert "0.55" in issues[0].detail


def test_layer4_no_ghost_when_prices_agree() -> None:
    """Book LOOKS like ghost shape but /price agrees with ask → not a ghost."""
    m = make_market("ng1", yes_token_id="t1", no_token_id=None)
    books = {"t1": {"asks": [{"price": "0.99"}], "bids": [{"price": "0.01"}]}}
    prices = {"t1": {"buy": "0.99"}}  # within 0.05 divergence of ask
    issues = layer4_cross_source([m], books_by_token=books, prices_by_token=prices)
    assert issues == []


def test_layer4_handles_missing_prices_reference_gracefully() -> None:
    """Ghost-shape book but no /price reference → cannot adjudicate, no issue raised."""
    m = make_market("nr1", yes_token_id="t1", no_token_id=None)
    books = {"t1": {"asks": [{"price": "0.99"}], "bids": [{"price": "0.01"}]}}
    prices = {"t1": None}  # explicit None: cannot detect without ground truth
    issues = layer4_cross_source([m], books_by_token=books, prices_by_token=prices)
    assert issues == []


def test_layer4_handles_two_tokens_per_market() -> None:
    """Both tokens present, only no_token_id missing in book → exactly 1 CLOB_MISSING."""
    m = make_market("two1", yes_token_id="t1", no_token_id="t2")
    books = {
        "t1": {"asks": [{"price": "0.55"}], "bids": [{"price": "0.45"}]},
        # t2 deliberately missing
    }
    prices = {"t1": {"buy": "0.55"}}
    issues = layer4_cross_source([m], books_by_token=books, prices_by_token=prices)
    assert len(issues) == 1
    assert issues[0].category == Category.CLOB_MISSING
    assert "t2" in issues[0].detail


# ============================================================================
# is_valid_overall — Phase 1 policy invariant
# ============================================================================


def test_is_valid_true_when_no_issues() -> None:
    assert is_valid_overall([]) is True


def test_is_valid_false_when_layer1_issue() -> None:
    issues = layer1_count(100, 99)  # produces 1 Layer 1 issue
    assert is_valid_overall(issues) is False


def test_is_valid_true_when_only_layer2_4_issues() -> None:
    """Layer 2 + Layer 4 issues are recorded but DO NOT flip is_valid (resolved Q5)."""
    issues = [
        Issue(layer=2, category=Category.ZOMBIE_MARKET, market_id="m1", detail="x"),
        Issue(layer=4, category=Category.GHOST_BOOK, market_id="m2", detail="y"),
        Issue(layer=4, category=Category.CLOB_MISSING, market_id="m3", detail="z"),
    ]
    assert is_valid_overall(issues) is True


# ============================================================================
# Sanity — Layer 4 unparseable book (F-1 invariant)
# ============================================================================


def test_layer4_unparseable_book_does_not_crash() -> None:
    """F-1: a book with non-numeric price MUST NOT crash the validator.

    This isn't one of the 17 plan-listed tests but exercises the F-1 security
    invariant directly. Kept as bonus coverage — the plan's done-criteria is
    'all 17 tests pass'; this is the 18th.
    """
    m = make_market("u1", yes_token_id="t1", no_token_id=None)
    books = {
        "t1": {
            "asks": [{"price": "not-a-number"}],
            "bids": [{"price": "0.45"}],
        }
    }
    prices = {"t1": {"buy": "0.55"}}
    issues = layer4_cross_source([m], books_by_token=books, prices_by_token=prices)
    # Either UNKNOWN issue raised OR no issue (depending on which side is unparseable
    # short-circuits). Either way: NO crash.
    for iss in issues:
        assert iss.layer == 4
        assert iss.category in (Category.UNKNOWN, Category.GHOST_BOOK, Category.CLOB_MISSING)
