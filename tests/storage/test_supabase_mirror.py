"""Tests for polyarb.storage.supabase_mirror narrow_market_row projection.

Phase 04 Plan 01 Task 2 (D-07): asserts the 11-column narrow projection
includes yes_token_id with correct nullable-passthrough semantics.

This file is intentionally narrow — it only covers narrow_market_row.
The fuller SupabaseMirror integration (push_snapshot / reconcile) is
already covered by tests/m1-perception/test_supabase_mirror.py.

Why a new file (not added to the m1-perception one): per Plan 01
acceptance criteria, this file is the canonical D-07 regression test
location. Keeping the narrow_market_row contract in a small dedicated
file makes future column additions (D-XX) straightforward to extend.
"""

from __future__ import annotations

import os

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


def _make_full_row(**overrides) -> dict:
    """Build a typical normalizer-shaped market dict.

    Keys match what `normalizer.normalize_market` emits — i.e. the dict
    shape consumed by supabase_mirror.narrow_market_row.
    """
    base = {
        "market_id": "mkt-1",
        "condition_id": "0xabc",
        "question": "Will X happen?",
        "slug": "will-x-happen",
        "event_slug": "event-x",
        "event_id": "evt-1",
        "mid_price": 0.5,
        "liquidity_usd": 1234.5,
        "volume_usd": 678.9,
        "end_time_ms": 1800000000000,
        "question_zh": None,
        "yes_token_id": "YES-123",
        "no_token_id": "NO-456",
        # Plus other normalizer fields that are NOT in narrow projection —
        # narrow_market_row should ignore them silently.
        "best_bid_price": 0.49,
        "best_ask_price": 0.51,
        "active": True,
        "closed": False,
        "fetched_at_ms": 1715500000000,
    }
    base.update(overrides)
    return base


def test_narrow_includes_yes_token_id_when_present() -> None:
    """D-07: narrow_market_row output dict must include yes_token_id key
    with the value from the source row."""
    from polyarb.storage.supabase_mirror import narrow_market_row

    full = _make_full_row(yes_token_id="YES-ABC-789")
    out = narrow_market_row(full, snapshot_id=42)

    assert "yes_token_id" in out, "yes_token_id must be a key in narrow projection"
    assert out["yes_token_id"] == "YES-ABC-789", (
        f"yes_token_id must passthrough source value; got {out['yes_token_id']!r}"
    )


def test_narrow_yes_token_id_none_when_absent() -> None:
    """D-07 nullable contract: when source row lacks yes_token_id (e.g. binary-resolved
    market with empty clobTokenIds), narrow projection must still include the key
    with value None (Supabase nullable column accepts NULL)."""
    from polyarb.storage.supabase_mirror import narrow_market_row

    full = _make_full_row()
    full.pop("yes_token_id")  # simulate market without clobTokenIds[0]
    out = narrow_market_row(full, snapshot_id=42)

    assert "yes_token_id" in out, "key must be present even when source value missing"
    assert out["yes_token_id"] is None, (
        f"missing yes_token_id must passthrough as None; got {out['yes_token_id']!r}"
    )


def test_narrow_yes_token_id_none_when_explicit_none() -> None:
    """When source explicitly stores yes_token_id=None (normalizer.py:107
    `else None` branch), narrow projection must also pass through None."""
    from polyarb.storage.supabase_mirror import narrow_market_row

    full = _make_full_row(yes_token_id=None)
    out = narrow_market_row(full, snapshot_id=42)

    assert out["yes_token_id"] is None


def test_narrow_no_token_id_passthrough_and_nullable() -> None:
    from polyarb.storage.supabase_mirror import narrow_market_row

    assert (
        narrow_market_row(_make_full_row(no_token_id="NO-X"), snapshot_id=42)["no_token_id"]
        == "NO-X"
    )

    missing = _make_full_row()
    missing.pop("no_token_id")
    assert narrow_market_row(missing, snapshot_id=42)["no_token_id"] is None
    assert (
        narrow_market_row(_make_full_row(no_token_id=None), snapshot_id=42)["no_token_id"] is None
    )


def test_narrow_projection_is_twelve_columns() -> None:
    """Phase 05.3 widens the narrow projection to both outcome tokens.

    Post-D-07 expected keys (snapshot_id is always present + projected from arg):
    market_id, question, slug, event_slug, mid_price, liquidity_usd,
    volume_usd, end_time_ms, snapshot_id, question_zh, yes_token_id.
    """
    from polyarb.storage.supabase_mirror import (
        _NARROW_MARKET_COLUMNS,
        narrow_market_row,
    )

    expected_columns = {
        "market_id",
        "question",
        "slug",
        "event_slug",
        "mid_price",
        "liquidity_usd",
        "volume_usd",
        "end_time_ms",
        "snapshot_id",
        "question_zh",
        "yes_token_id",
        "no_token_id",
    }
    assert set(_NARROW_MARKET_COLUMNS) == expected_columns, (
        f"_NARROW_MARKET_COLUMNS must be 12-column post-05.3; got {set(_NARROW_MARKET_COLUMNS)!r}"
    )
    assert len(_NARROW_MARKET_COLUMNS) == 12, (
        f"_NARROW_MARKET_COLUMNS must have exactly 12 entries; got {len(_NARROW_MARKET_COLUMNS)}"
    )

    full = _make_full_row()
    out = narrow_market_row(full, snapshot_id=42)
    assert set(out.keys()) == expected_columns


def test_narrow_event_slug_special_case_still_works() -> None:
    """Regression: D-07 must not break the event_slug -> event_id fallback
    (the only existing special-case branch in narrow_market_row)."""
    from polyarb.storage.supabase_mirror import narrow_market_row

    # Source has event_id but no event_slug — should fall back to event_id.
    full = _make_full_row()
    full.pop("event_slug")
    full["event_id"] = "evt-fallback-9"
    out = narrow_market_row(full, snapshot_id=42)
    assert out["event_slug"] == "evt-fallback-9"


def test_narrow_snapshot_id_overrides_full_row() -> None:
    """Regression: snapshot_id always comes from the function arg, not the
    full_row dict (matches existing pre-D-07 behaviour)."""
    from polyarb.storage.supabase_mirror import narrow_market_row

    full = _make_full_row()
    full["snapshot_id"] = 999  # source-row snapshot_id should be ignored
    out = narrow_market_row(full, snapshot_id=42)
    assert out["snapshot_id"] == 42
