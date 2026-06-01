---
quick_id: 260601-depth-yes-usd
type: quick
mode: tdd
ws: m1-perception
created: 2026-06-01
files_modified:
  - src/polyarb/daemon/l2_main.py
  - tests/m1-perception/test_l2_main_book_levels.py
---

# Quick Task: fill `depth_yes_usd` / `depth_no_usd` in `_tob_row_from_frame`

## Problem (root cause, confirmed 2026-06-01 prod query — see SESSION 34 EOD diagnosis)

`_tob_row_from_frame` (`src/polyarb/daemon/l2_main.py:176-177`) returns `depth_yes_usd: None` and `depth_no_usd: None` for ALL frames. Comment says "populated only when book frame carries size" but the code path never populates them.

Lifetime stats from prod Supabase `l2_top_of_book` (2026-05-24 → 2026-06-01):
- 631 total rows
- `with_depth = 0` (zero rows ever had depth_yes_usd > 0)
- All `source_event = 'book'` (every row is from a `book` snapshot, which carries `bids[]` / `asks[]` arrays with `{price, size}`)

This is a Phase 03 latent TODO surfaced when Phase 05 L3 promoter ran in prod (`promote_run: +0 -0 markets=0 tokens=0` — D-13 threshold `depth_yes_usd > 500` can never fire). 24h soak verdict (D-12) is impossible to satisfy until depth is populated.

## Design (locked — straightforward fill from book event arrays)

The Polymarket WS `book` event carries top-N orderbook levels:
```json
{
  "event_type": "book",
  "asset_id": "...",
  "bids": [{"price": "0.45", "size": "1234.56"}, {"price": "0.44", "size": "..."}, ...],
  "asks": [{"price": "0.46", "size": "987.65"}, ...]
}
```

USD-denominated depth at top-N levels:
- `depth_yes_usd = sum(level.price * level.size for level in bids[:N])` — total USD value of bids near the bid
- `depth_no_usd = sum(level.price * level.size for level in asks[:N])` — total USD value of asks near the ask

**N = top-10 levels per side** (matches CONTEXT D-07 for `l2_book_levels`). If fewer than 10 levels available, sum what's there. Per-level guards: skip levels with `size <= 0` or non-numeric `price`/`size`.

Non-`book` events (`price_change`, `best_bid_ask`, `last_trade_price`) don't carry depth — leave `depth_yes_usd = depth_no_usd = None` for those. The chain-truth: `source_event = 'book'` → depth filled; other `source_event` → depth NULL.

## Constraints honored

- **Phase 03 schema lockstep**: `l2_top_of_book.depth_yes_usd` / `depth_no_usd` columns already exist (NUMERIC nullable, `alembic 003`); no migration needed.
- **No behavior change for non-`book` events** — existing `price_change` / `best_bid_ask` paths untouched.
- **GAP-401 lock**: this is l2_main.py, not ws_watchdog.py — out of scope. No watchdog touch.
- **D-12 unblocking, not D-13 patching** — `depth_yes_usd > 500` threshold stays, just now the column is populated correctly so the threshold can fire.
- **TDD discipline**: 4 RED tests added to existing `tests/m1-perception/test_l2_main_book_levels.py`:
  1. book event with bids[10]/asks[10] → depth_yes_usd = sum(price*size for top-10 bids), depth_no_usd = sum for asks
  2. book event with bids[3] only → depth_yes_usd sums 3 levels (not crash)
  3. book event with empty bids → depth_yes_usd = None (matches existing behavior for missing data)
  4. `price_change` event → depth_yes_usd = None (depth only populated for book events)

## Wave 0 — RED tests

Append to `tests/m1-perception/test_l2_main_book_levels.py`:
```python
def test_tob_row_book_event_fills_depth_yes_usd_from_top_10_bids():
    """book event with bids/asks arrays → depth_yes_usd = sum(price*size top-10)."""
    frame = {
        "event_type": "book",
        "asset_id": "AID-1",
        "timestamp": 1717243200,
        "bids": [{"price": "0.45", "size": "1000"}, {"price": "0.44", "size": "500"}],
        "asks": [{"price": "0.46", "size": "800"}, {"price": "0.47", "size": "200"}],
    }
    row = _tob_row_from_frame(frame)
    assert row is not None
    assert row["depth_yes_usd"] == pytest.approx(0.45 * 1000 + 0.44 * 500)  # 670.0
    assert row["depth_no_usd"] == pytest.approx(0.46 * 800 + 0.47 * 200)  # 462.0


def test_tob_row_book_event_caps_at_top_10_levels():
    """If bids has 15 levels, only top-10 contribute to depth_yes_usd."""
    frame = {
        "event_type": "book",
        "asset_id": "AID-1",
        "timestamp": 1717243200,
        "bids": [{"price": "0.5", "size": "100"} for _ in range(15)],
        "asks": [],
    }
    row = _tob_row_from_frame(frame)
    assert row is not None
    assert row["depth_yes_usd"] == pytest.approx(0.5 * 100 * 10)  # only top-10 = 500.0
    assert row["depth_no_usd"] is None or row["depth_no_usd"] == 0.0


def test_tob_row_book_event_skips_zero_size_levels():
    """Levels with size=0 are excluded from depth sum."""
    frame = {
        "event_type": "book",
        "asset_id": "AID-1",
        "timestamp": 1717243200,
        "bids": [{"price": "0.5", "size": "100"}, {"price": "0.4", "size": "0"}, {"price": "0.3", "size": "200"}],
        "asks": [],
    }
    row = _tob_row_from_frame(frame)
    assert row is not None
    # 0.5*100 + 0.3*200 = 50 + 60 = 110 (the 0-size level is skipped)
    assert row["depth_yes_usd"] == pytest.approx(110.0)


def test_tob_row_price_change_event_leaves_depth_none():
    """Non-book events do NOT populate depth (Polymarket WS only book carries depth)."""
    frame = {
        "event_type": "price_change",
        "asset_id": "AID-1",
        "timestamp": 1717243200,
        "side": "BUY",
        "price": "0.55",
        "size": "100",
    }
    row = _tob_row_from_frame(frame)
    assert row is not None
    assert row["depth_yes_usd"] is None
    assert row["depth_no_usd"] is None
```

## Wave 1 — GREEN implementation

In `src/polyarb/daemon/l2_main.py` `_tob_row_from_frame`:

1. Add helper above the function (or inline):
```python
def _sum_depth_usd(levels: list, top_n: int = 10) -> float | None:
    """Sum (price * size) for top-N levels; skip non-numeric or zero-size; None if no valid levels."""
    if not levels:
        return None
    total = 0.0
    n_valid = 0
    for entry in levels[:top_n]:
        if not isinstance(entry, dict):
            continue
        try:
            price = float(entry.get("price")) if entry.get("price") is not None else None
            size = float(entry.get("size")) if entry.get("size") is not None else None
        except (TypeError, ValueError):
            continue
        if price is None or size is None or size <= 0:
            continue
        total += price * size
        n_valid += 1
    return total if n_valid > 0 else None
```

2. In the `et == "book"` branch (currently lines 152-158), compute depth alongside best_bid/best_ask:
```python
depth_yes_usd_v = None
depth_no_usd_v = None
if et == "book":
    bids = frame.get("bids") or []
    asks = frame.get("asks") or []
    if bids and isinstance(bids[0], dict):
        best_bid = bids[0].get("price", best_bid)
    if asks and isinstance(asks[0], dict):
        best_ask = asks[0].get("price", best_ask)
    depth_yes_usd_v = _sum_depth_usd(bids, top_n=10)
    depth_no_usd_v = _sum_depth_usd(asks, top_n=10)
```

3. Update the return dict line 176-177:
```python
"depth_yes_usd": depth_yes_usd_v,
"depth_no_usd": depth_no_usd_v,
```

## Verification (after deploy)

After `flyctl deploy` + ~3min:
- `make ohlc-spot-check URL=https://polyarb-l2.fly.dev` — `/health` `l3:active_count` may still 0 (need 1 more 5min promoter cycle after depth populates)
- Direct prod query: `SELECT COUNT(*) FROM l2_top_of_book WHERE depth_yes_usd > 0 AND ts > now() - interval '5 min'` should return >0
- Within 1-2 promote cycles (5-10min): `/health` `l3:active_count` grows from 0/10 → 1-10 (depending on how many markets pass D-13)

## Done (success criteria)

- [ ] All 4 new RED tests in test_l2_main_book_levels.py go RED (verify they fail before impl)
- [ ] After GREEN impl, all tests green (29 total: 25 existing + 4 new)
- [ ] All existing tests still green: test_alembic_005_ohlc_views / test_l3_promoter / test_l2_health_l3_subchecks / test_ws_consumer_dynamic_subscribe / test_ws_watchdog_liveness
- [ ] After redeploy: prod `l2_top_of_book.depth_yes_usd > 0` for at least some rows within 5min
- [ ] After 5-15min: `/health l3:active_count > 0` (promoter starts picking markets)
- [ ] SUMMARY.md committed
- [ ] STATE.md quick tasks table updated
