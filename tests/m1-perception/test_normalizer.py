"""Tests for polyarb.snapshot.normalizer.

Plan 01-5 T2 — Gamma raw dict edge cases:
  - Pitfall 2: clobTokenIds and outcomePrices are JSON-encoded strings
  - Pitfall 3: token IDs MUST stay as str (uint256 overflow int64)
  - F-8: naive endDate datetime treated as UTC
  - liquidity field fallback (Open Q#5)
"""
from __future__ import annotations

from polyarb.snapshot.normalizer import normalize_market


# Expected output keys (everything in MARKETS_COLUMN_ORDER except snapshot_id).
# Phase 1.1 T1 added: category + tags.
EXPECTED_KEYS = {
    "market_id",
    "condition_id",
    "slug",
    "question",
    "yes_token_id",
    "no_token_id",
    "mid_price",
    "liquidity_usd",
    "volume_usd",
    "best_bid_price",
    "best_bid_size",
    "best_ask_price",
    "best_ask_size",
    "end_time_ms",
    "active",
    "closed",
    "neg_risk",
    "neg_risk_market_id",
    "fetched_at_ms",
    "incomplete",
    "category",  # Phase 1.1 T1
    "tags",      # Phase 1.1 T1
}


def make_raw(**overrides) -> dict:
    """Build a 'good' Gamma raw dict that overrides[k]=v can mutate."""
    base = {
        "id": "M-1",
        "conditionId": "0xabc",
        "slug": "test",
        "question": "Q?",
        # JSON-string of two long uint256 token ids (Pitfall 2 + 3).
        "clobTokenIds": '["' + "1" * 70 + '", "' + "2" * 70 + '"]',
        "outcomePrices": '["0.6", "0.4"]',
        "liquidityNum": 1500.0,
        "volumeNum": 50000.0,
        "endDate": "2026-12-31T00:00:00Z",
        "active": True,
        "closed": False,
        "negRisk": False,
    }
    base.update(overrides)
    return base


# ─────────────────────────────────────────────────────────────────────────────
# Happy path + key-set contract
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_happy_path() -> None:
    out = normalize_market(make_raw())
    assert out is not None
    # Key-set contract: every output dict has exactly the 20 EXPECTED_KEYS.
    assert set(out.keys()) == EXPECTED_KEYS
    assert out["market_id"] == "M-1"
    # uint256 preserved as 70-char string (Pitfall 3).
    assert isinstance(out["yes_token_id"], str)
    assert len(out["yes_token_id"]) == 70
    assert out["yes_token_id"] != out["no_token_id"]
    assert isinstance(out["no_token_id"], str)
    assert out["mid_price"] == 0.6
    assert out["liquidity_usd"] == 1500.0
    assert out["volume_usd"] == 50000.0
    # endDate "2026-12-31T00:00:00Z" → epoch ms > 1.7e12 sanity.
    assert isinstance(out["end_time_ms"], int)
    assert out["end_time_ms"] > 1_700_000_000_000
    # CLOB-derived fields are None placeholders (orchestrator overwrites).
    for k in (
        "best_bid_price",
        "best_bid_size",
        "best_ask_price",
        "best_ask_size",
        "fetched_at_ms",
    ):
        assert out[k] is None, f"{k} should be None placeholder pre-CLOB"
    assert out["incomplete"] is False
    assert out["active"] is True
    assert out["closed"] is False
    assert out["neg_risk"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Missing-id handling
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_missing_id_returns_none() -> None:
    assert normalize_market(make_raw(id=None)) is None
    assert normalize_market({}) is None
    # Empty string is also unrecoverable (no PK).
    assert normalize_market(make_raw(id="")) is None


# ─────────────────────────────────────────────────────────────────────────────
# Pitfall 2: clobTokenIds as JSON-string
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_clobTokenIds_string_form() -> None:
    """JSON-string form must be json.loads()'d (Pitfall 2)."""
    raw = make_raw(clobTokenIds='["abc123", "def456"]')
    out = normalize_market(raw)
    assert out is not None
    assert out["yes_token_id"] == "abc123"
    assert out["no_token_id"] == "def456"


def test_normalize_clobTokenIds_already_list() -> None:
    """Defensive: if Gamma ever returns a real list, normalizer must still work."""
    raw = make_raw(clobTokenIds=["t1", "t2"])
    out = normalize_market(raw)
    assert out is not None
    assert out["yes_token_id"] == "t1"
    assert out["no_token_id"] == "t2"


def test_normalize_clobTokenIds_malformed_json() -> None:
    """Malformed JSON string: graceful — yields None token ids, no exception."""
    raw = make_raw(clobTokenIds="not-valid-json")
    out = normalize_market(raw)
    assert out is not None
    assert out["yes_token_id"] is None
    assert out["no_token_id"] is None


def test_normalize_outcomePrices_malformed() -> None:
    """Malformed outcomePrices: mid_price falls back to None."""
    raw = make_raw(outcomePrices="bad")
    out = normalize_market(raw)
    assert out is not None
    assert out["mid_price"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Open Q#5: liquidity field fallback
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_liquidity_fallback_to_string_field() -> None:
    """If liquidityNum is missing, fall back to ``liquidity`` (string)."""
    raw = make_raw(liquidityNum=None, liquidity="2500.5")
    out = normalize_market(raw)
    assert out is not None
    assert out["liquidity_usd"] == 2500.5


def test_normalize_liquidity_neither_field_set() -> None:
    """Neither liquidityNum nor liquidity → None (downstream filters handle it)."""
    raw = make_raw(liquidityNum=None)
    raw.pop("liquidity", None)
    out = normalize_market(raw)
    assert out is not None
    assert out["liquidity_usd"] is None


# ─────────────────────────────────────────────────────────────────────────────
# F-8: endDate ISO parsing
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_endDate_naive_iso() -> None:
    """Naive ISO-8601 (no Z, no tz): treated as UTC per F-8 (not local time)."""
    raw = make_raw(endDate="2026-12-31T00:00:00")
    out = normalize_market(raw)
    assert out is not None
    assert out["end_time_ms"] is not None
    # Same as "2026-12-31T00:00:00Z" — both UTC.
    raw_utc = make_raw(endDate="2026-12-31T00:00:00Z")
    out_utc = normalize_market(raw_utc)
    assert out_utc is not None
    assert out["end_time_ms"] == out_utc["end_time_ms"]


def test_normalize_endDate_malformed() -> None:
    """Malformed endDate yields None — never raises."""
    raw = make_raw(endDate="invalid")
    out = normalize_market(raw)
    assert out is not None
    assert out["end_time_ms"] is None


def test_normalize_endDate_missing() -> None:
    """Missing endDate also yields None (key absent, not just empty)."""
    raw = make_raw()
    raw.pop("endDate")
    out = normalize_market(raw)
    assert out is not None
    assert out["end_time_ms"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Pitfall 3: token IDs as str (uint256 overflow guard)
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_token_id_preserves_uint256_string() -> None:
    """75-digit token id stays as str — no int conversion."""
    huge = "1" * 75
    raw = make_raw(clobTokenIds=f'["{huge}", "2"]')
    out = normalize_market(raw)
    assert out is not None
    assert out["yes_token_id"] == huge
    assert isinstance(out["yes_token_id"], str)


# ─────────────────────────────────────────────────────────────────────────────
# Real recorded fixture round-trip
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_real_fixture_sample(gamma_fixture: list[dict]) -> None:
    """Every market in the recorded gamma_sample.json normalizes successfully."""
    assert len(gamma_fixture) > 0, "fixture should be non-empty"
    for raw in gamma_fixture:
        out = normalize_market(raw)
        # All recorded fixtures have ``id`` populated, so no None.
        assert out is not None, f"normalize returned None for {raw.get('slug')}"
        assert set(out.keys()) == EXPECTED_KEYS
        # Token IDs must be stringy (Pitfall 3) when present.
        if out["yes_token_id"] is not None:
            assert isinstance(out["yes_token_id"], str)
        if out["no_token_id"] is not None:
            assert isinstance(out["no_token_id"], str)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1.1 T1: category + tags extraction
# ─────────────────────────────────────────────────────────────────────────────


def test_category_extracted() -> None:
    """category is a single string from raw['category']."""
    out = normalize_market(make_raw(category="Politics"))
    assert out is not None
    assert out["category"] == "Politics"


def test_category_missing_returns_none() -> None:
    """Missing category → None (downstream LEFT JOIN handles NULL)."""
    raw = make_raw()
    raw.pop("category", None)
    out = normalize_market(raw)
    assert out is not None
    assert out["category"] is None


def test_tags_serialized_as_json() -> None:
    """tags is a list[str] from Gamma — stored as JSON-encoded string."""
    out = normalize_market(make_raw(tags=["a", "b"]))
    assert out is not None
    # ensure_ascii=False but ASCII inputs serialize the same way: '["a", "b"]'.
    import json
    parsed = json.loads(out["tags"])
    assert parsed == ["a", "b"]


def test_tags_missing_returns_empty_array_string() -> None:
    """Missing tags → '[]' (empty array string), not None — schema column is TEXT non-null in payload."""
    raw = make_raw()
    raw.pop("tags", None)
    out = normalize_market(raw)
    assert out is not None
    assert out["tags"] == "[]"


def test_tags_unicode_preserved() -> None:
    """ensure_ascii=False keeps CJK chars readable in the stored string."""
    out = normalize_market(make_raw(tags=["体育", "Politics"]))
    assert out is not None
    assert "体育" in out["tags"]
    # Must still be valid JSON.
    import json
    parsed = json.loads(out["tags"])
    assert parsed == ["体育", "Politics"]


def test_tags_none_returns_empty_array_string() -> None:
    """raw['tags']=None should not crash json.dumps — defaults to []."""
    out = normalize_market(make_raw(tags=None))
    assert out is not None
    assert out["tags"] == "[]"
