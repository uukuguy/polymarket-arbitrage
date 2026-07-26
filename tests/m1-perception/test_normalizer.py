"""Tests for polyarb.snapshot.normalizer.

Plan 01-5 T2 — Gamma raw dict edge cases:
  - Pitfall 2: clobTokenIds and outcomePrices are JSON-encoded strings
  - Pitfall 3: token IDs MUST stay as str (uint256 overflow int64)
  - F-8: naive endDate datetime treated as UTC
  - liquidity field fallback (Open Q#5)

Phase 1.1 Amendment 01:
  - normalize_market drops category/tags extraction (these never had data)
  - normalize_market accepts market_to_event_map → writes event_id column
  - new normalize_events: Gamma /events raw → events + event_tags + reverse map
"""

from __future__ import annotations

import pytest

from polyarb.perception.market_truth import CONFLICTING_EVENT_MEMBERSHIP_REASON
from polyarb.snapshot.normalizer import normalize_events, normalize_market

# Expected output keys (everything in MARKETS_COLUMN_ORDER except snapshot_id).
# Phase 1.1 Amendment 01: -category -tags +event_id
# Phase 02 Plan 01: +page_fetched_at_ms (per-page real fetch time, fixes L2)
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
    "page_fetched_at_ms",  # Phase 02 Plan 01: per-page real fetch time (nullable)
    "incomplete",
    "event_id",  # Phase 1.1 Amendment 01
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
    # Key-set contract: every output dict has exactly the EXPECTED_KEYS.
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
    # Phase 1.1 Amendment 01: no map → event_id is None
    assert out["event_id"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Missing-id handling
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_missing_id_returns_none() -> None:
    assert normalize_market(make_raw(id=None)) is None
    assert normalize_market({}) is None
    # Empty string is also unrecoverable (no PK).
    assert normalize_market(make_raw(id="")) is None


def test_normalize_market_strips_authoritative_market_id_before_lookup() -> None:
    out = normalize_market(
        make_raw(id="  M-42  "),
        market_to_event_map={"M-42": "EV-7"},
    )

    assert out is not None
    assert out["market_id"] == "M-42"
    assert out["event_id"] == "EV-7"


@pytest.mark.parametrize("invalid_id", [False, True, 7, [], {}, "   "])
def test_normalize_market_rejects_non_string_or_blank_identity(
    invalid_id: object,
) -> None:
    assert normalize_market(make_raw(id=invalid_id)) is None


@pytest.mark.parametrize(
    ("raw_field", "normalized_field"),
    [
        ("active", "active"),
        ("closed", "closed"),
        ("negRisk", "neg_risk"),
    ],
)
@pytest.mark.parametrize("invalid_value", [None, 0, 1, "false", [], {}])
def test_normalize_market_preserves_malformed_boolean_as_unknown(
    raw_field: str,
    normalized_field: str,
    invalid_value: object,
) -> None:
    out = normalize_market(make_raw(**{raw_field: invalid_value}))

    assert out is not None
    assert out[normalized_field] is None


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
# Phase 1.1 Amendment 01 — event_id injection from market_to_event_map
# ─────────────────────────────────────────────────────────────────────────────


def test_normalize_with_event_map_assigns_event_id() -> None:
    """When market_to_event_map has the market_id, event_id flows through."""
    out = normalize_market(make_raw(id="M-42"), market_to_event_map={"M-42": "EV-7"})
    assert out is not None
    assert out["event_id"] == "EV-7"


def test_normalize_with_event_map_missing_market_returns_none_event_id() -> None:
    """Market not in the map → event_id None (orphan markets are tolerated)."""
    out = normalize_market(make_raw(id="M-orphan"), market_to_event_map={"M-other": "EV-1"})
    assert out is not None
    assert out["event_id"] is None


def test_normalize_no_map_arg_yields_none_event_id() -> None:
    """Default (no map) → event_id None for backward compat with mocked tests."""
    out = normalize_market(make_raw())
    assert out is not None
    assert out["event_id"] is None


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1.1 Amendment 01 — normalize_events
# ─────────────────────────────────────────────────────────────────────────────


def make_event(idx: int, n_markets: int = 2, n_tags: int = 3) -> dict:
    """Lightweight Gamma /events row for testing."""
    return {
        "id": str(16000 + idx),
        "slug": f"event-{idx}",
        "title": f"Event {idx}",
        "ticker": f"TKR-{idx}",
        "active": True,
        "closed": False,
        "liquidity": 1234.5,
        "volume": 6789.0,
        "endDate": "2026-12-31T00:00:00Z",
        "tags": [
            {"id": str(100 + j), "label": f"Tag{j}", "slug": f"tag{j}"} for j in range(n_tags)
        ],
        "markets": [{"id": str(540000 + idx * 10 + k)} for k in range(n_markets)],
    }


def test_normalize_events_happy_path() -> None:
    """3 events × 2 markets × 3 tags → 3 events_rows + 9 event_tags + 6-entry map."""
    raw_events = [make_event(i) for i in range(3)]
    events, event_tags, m2e, members, groups = normalize_events(raw_events)

    assert len(events) == 3
    assert len(event_tags) == 9  # 3 events × 3 tags each
    assert len(m2e) == 6  # 3 events × 2 markets each
    assert members == []
    assert groups == []

    # Spot-check event row shape
    ev = events[0]
    assert ev["id"] == "16000"
    assert ev["slug"] == "event-0"
    assert ev["title"] == "Event 0"
    assert ev["ticker"] == "TKR-0"
    assert ev["active"] is True
    assert ev["closed"] is False
    assert ev["liquidity_usd"] == 1234.5
    assert ev["volume_usd"] == 6789.0
    assert ev["end_time_ms"] is not None
    assert ev["fetched_at_ms"] is None  # orchestrator stamps later

    # Tag row shape
    tag = event_tags[0]
    assert set(tag.keys()) == {"event_id", "tag_id", "tag_label", "tag_slug"}
    assert tag["event_id"] == "16000"

    # market→event map sanity
    assert m2e["540000"] == "16000"
    assert m2e["540001"] == "16000"
    assert m2e["540010"] == "16001"


def test_normalize_events_missing_id_skipped() -> None:
    """Event with no id is skipped; valid events still processed."""
    raw_events = [
        {"id": None, "slug": "bad"},
        make_event(1),
        {"slug": "no-id-key"},
    ]
    events, event_tags, m2e, members, groups = normalize_events(raw_events)
    assert len(events) == 1
    assert events[0]["id"] == "16001"
    # 3 tags from the one good event + 2 markets in map
    assert len(event_tags) == 3
    assert len(m2e) == 2
    assert members == []
    assert groups == []


def test_normalize_events_no_tags_array() -> None:
    """Event with missing/non-list tags → no event_tags rows but event still recorded."""
    raw = make_event(0)
    raw["tags"] = None
    events, event_tags, _, _, _ = normalize_events([raw])
    assert len(events) == 1
    assert len(event_tags) == 0


def test_normalize_events_skips_incomplete_tags() -> None:
    """Tag missing label/slug/id is silently dropped (NOT NULL schema)."""
    raw = make_event(0, n_tags=0)
    raw["tags"] = [
        {"id": "1", "label": "Good", "slug": "good"},
        {"id": "2", "label": None, "slug": "no-label"},  # incomplete
        {"id": None, "label": "X", "slug": "no-id"},  # incomplete
        {"id": "4", "label": "Y", "slug": ""},  # incomplete (empty slug)
        "not-a-dict",  # garbage
    ]
    _, event_tags, _, _, _ = normalize_events([raw])
    assert len(event_tags) == 1
    assert event_tags[0]["tag_id"] == "1"


def test_normalize_events_no_markets_array() -> None:
    """Event with missing markets array → no map entries, but event recorded."""
    raw = make_event(0)
    raw["markets"] = None
    events, _, m2e, members, groups = normalize_events([raw])
    assert len(events) == 1
    assert len(m2e) == 0
    assert members == []
    assert groups == []


def test_normalize_events_dedupe_tag_within_event() -> None:
    """Same tag_id appearing twice in one event's tags → only one event_tags row."""
    raw = make_event(0, n_tags=0)
    raw["tags"] = [
        {"id": "120", "label": "Finance", "slug": "finance"},
        {"id": "120", "label": "Finance", "slug": "finance"},  # duplicate
    ]
    _, event_tags, _, _, _ = normalize_events([raw])
    assert len(event_tags) == 1


def test_normalize_events_first_event_wins_for_market() -> None:
    """If a market_id appears in multiple events, FIRST event_id is kept (defensive)."""
    raw0 = make_event(0, n_markets=0)
    raw0["id"] = "EV-A"
    raw0["markets"] = [{"id": "M-shared"}]

    raw1 = make_event(1, n_markets=0)
    raw1["id"] = "EV-B"
    raw1["markets"] = [{"id": "M-shared"}]

    _, _, m2e, _, _ = normalize_events([raw0, raw1])
    assert m2e["M-shared"] == "EV-A"


def test_market_shared_across_events_marks_every_neg_risk_group_incomplete() -> None:
    raw0 = make_event(0, n_markets=0)
    raw0.update(
        {
            "id": "EV-A",
            "negRisk": True,
            "negRiskAugmented": False,
            "negRiskMarketID": "group-a",
            "markets": [
                {
                    "id": "M-shared",
                    "active": True,
                    "closed": False,
                    "negRiskOther": False,
                },
                {
                    "id": "M-a",
                    "active": True,
                    "closed": False,
                    "negRiskOther": False,
                },
            ],
        }
    )
    raw1 = make_event(1, n_markets=0)
    raw1.update(
        {
            "id": "EV-B",
            "negRisk": True,
            "negRiskAugmented": False,
            "negRiskMarketID": "group-b",
            "markets": [
                {
                    "id": "M-shared",
                    "active": True,
                    "closed": False,
                    "negRiskOther": False,
                },
                {
                    "id": "M-b",
                    "active": True,
                    "closed": False,
                    "negRiskOther": False,
                },
            ],
        }
    )

    _, _, market_to_event, members, groups = normalize_events([raw0, raw1])

    assert market_to_event["M-shared"] == "EV-A"
    assert len(members) == 4
    assert [(group.event_id, group.quality, group.reason) for group in groups] == [
        ("EV-A", "incomplete-source", CONFLICTING_EVENT_MEMBERSHIP_REASON),
        ("EV-B", "incomplete-source", CONFLICTING_EVENT_MEMBERSHIP_REASON),
    ]


def test_normalize_events_empty_input() -> None:
    """Empty input → all empty outputs."""
    events, event_tags, m2e, members, groups = normalize_events([])
    assert events == []
    assert event_tags == []
    assert m2e == {}
    assert members == []
    assert groups == []


def test_normalize_events_dedupe_event_id_across_batch() -> None:
    """Same event_id appearing twice in raw_events → single events_row (defense
    against Gamma /events pagination duplicates, parallel to /markets ~4% dups).
    """
    raw_events = [make_event(0), make_event(0)]  # same id twice
    events, _, _, _, _ = normalize_events(raw_events)
    assert len(events) == 1


def test_normalize_events_slug_fallback_to_id() -> None:
    """If event has no slug, use id (defensive — schema requires NOT NULL slug)."""
    raw = make_event(0)
    raw["slug"] = None
    events, _, _, _, _ = normalize_events([raw])
    assert len(events) == 1
    assert events[0]["slug"] == "16000"


def test_normalize_events_liquidity_fallback() -> None:
    """liquidityNum / volumeNum used as fallback when liquidity / volume absent."""
    raw = make_event(0)
    raw["liquidity"] = None
    raw["liquidityNum"] = 9999.9
    raw["volume"] = None
    raw["volumeNum"] = 8888.8
    events, _, _, _, _ = normalize_events([raw])
    assert events[0]["liquidity_usd"] == 9999.9
    assert events[0]["volume_usd"] == 8888.8


# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 01 — page_fetched_at_ms per-page stamp (Wave 0 RED tests)
# ─────────────────────────────────────────────────────────────────────────────


def test_page_fetched_at_ms_carried_from_raw() -> None:
    """Phase 02: _page_fetched_at_ms private key on raw dict must flow through
    to page_fetched_at_ms in the normalized market row (per-page stamp, fixes L2).

    Test 1.1 from 02-01-PLAN.md Wave 0 requirements.
    """
    # Case 1: raw dict has _page_fetched_at_ms → carried through
    raw_with_stamp = make_raw(_page_fetched_at_ms=1715500000000)
    out = normalize_market(raw_with_stamp)
    assert out is not None
    assert "page_fetched_at_ms" in out, (
        "normalize_market must include page_fetched_at_ms key in output"
    )
    assert out["page_fetched_at_ms"] == 1715500000000, (
        f"Expected 1715500000000, got {out['page_fetched_at_ms']}"
    )

    # Case 2: raw dict WITHOUT _page_fetched_at_ms → page_fetched_at_ms is None (nullable)
    raw_no_stamp = make_raw()
    out_no_stamp = normalize_market(raw_no_stamp)
    assert out_no_stamp is not None
    assert "page_fetched_at_ms" in out_no_stamp, (
        "normalize_market must always include page_fetched_at_ms key (nullable)"
    )
    assert out_no_stamp["page_fetched_at_ms"] is None, (
        f"Expected None when _page_fetched_at_ms absent, got {out_no_stamp['page_fetched_at_ms']}"
    )
