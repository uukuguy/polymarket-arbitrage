---
quick_id: 260601-depth-yes-usd
type: quick
ws: m1-perception
status: complete
completed: 2026-06-01
commits:
  - facbffd test(quick-260601-depth-yes-usd): RED — 4 tests for _tob_row_from_frame depth fill
  - 9122b39 feat(quick-260601-depth-yes-usd): GREEN — fill depth_yes_usd / depth_no_usd from book event levels
files_modified:
  - src/polyarb/daemon/l2_main.py (added _sum_depth_usd helper + populate depth in book branch)
  - tests/m1-perception/test_l2_main_book_levels.py (+4 RED→GREEN tests)
---

# Quick Task SUMMARY: depth_yes_usd / depth_no_usd fill in `_tob_row_from_frame`

## Context (discovered SESSION 34 EOD, post Phase 05 deploy)

Right after `flyctl deploy` of Phase 05 (v22) to prod polyarb-l2, `/health` surfaced 3 new L3 sub-checks correctly:
- `l3:active_count = 0/10` (warn, under-filled)
- `l3:last_promote_at_s = 56s` (pass, promoter ran)
- `l3:last_book_levels_write_at_s = null` (cold-start)

Diagnosed via direct Supabase query: **`l2_top_of_book.depth_yes_usd` had been NULL for ALL 631 lifetime rows** (2026-05-24 → 2026-06-01). Root cause: Phase 03 `_tob_row_from_frame` returned `depth_yes_usd: None` as a TODO that was never closed. D-13 promoter threshold `depth_yes_usd > 500` therefore could never fire — 24h soak (D-12) was unsatisfiable in current state.

## Resolution

Added `_sum_depth_usd(levels, top_n=10)` helper that sums `price * size` over top-10 orderbook levels, skipping non-dict / non-numeric / zero-size entries. Called in the `et == "book"` branch of `_tob_row_from_frame` to populate `depth_yes_usd` (from bids) and `depth_no_usd` (from asks). Non-`book` events (price_change, best_bid_ask) leave depth None — chain-truth: depth populated <=> source was a book frame.

## Tests

4 new tests in `tests/m1-perception/test_l2_main_book_levels.py`:
1. `test_tob_row_book_event_fills_depth_yes_usd_from_top_10_bids` — sum of price×size for bids=2 levels, asks=2 levels
2. `test_tob_row_book_event_caps_at_top_10_levels` — bids=15 → only top-10 contribute
3. `test_tob_row_book_event_skips_zero_size_levels` — size=0 and size<0 are excluded
4. `test_tob_row_price_change_event_leaves_depth_none` — non-book events stay None

RED → GREEN cycle: RED commit facbffd (1 of 4 tests fails as expected — others not reached due to `-x`), GREEN commit 9122b39 (all 4 pass).

## Regression

Full Phase 05 test suite green after fix:
- `test_l2_main_book_levels.py`: 13/13 (9 existing + 4 new)
- `test_l3_promoter.py`: 12/12
- `test_l2_health_l3_subchecks.py`: 13/13
- `test_l2_supabase_mirror_book_levels.py`: 7/7
- `test_alembic_005_ohlc_views.py`: 6/6
- `test_ws_consumer_dynamic_subscribe.py`: 9/9
- `test_candidate_refresh_l3_protection.py`: 2/2
- `test_ws_watchdog_liveness.py`: 10/10
- `tests/observation/test_l2_candidate_refresh.py`: 21/21
- `test_l2_supabase_mirror_persist.py`: 3/3
- **Total: 96/96 green**

## Next step

- Redeploy `flyctl deploy --config fly-l2.toml --remote-only` to ship the depth fill to prod
- Wait ~5-10min for L2 mirror to refresh + verify prod `l2_top_of_book.depth_yes_usd > 0` for recent rows
- Wait 1-2 more L3 promoter cycles (5-10min) and check `/health l3:active_count > 0`
- Then Wave 5 Task 2 (24h soak) can proceed with realistic chance of D-12 PASS

## Pre-existing bugs found / impact

This fix surfaces and resolves a 7-day latent Phase 03 TODO. Without it, every downstream consumer of `depth_yes_usd` would have observed it as always-NULL — including future strategies that filter on depth, dashboards rendering depth metrics, IMDEA-style arb screens. The bug was hidden because L2 itself was only "Status:pass" on staleness checks (which it was — mirror was pushing rows, just rows with `depth_yes_usd=NULL`).
