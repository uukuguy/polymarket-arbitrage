---
quick_id: 260602-ws-dynamic-subscribe
type: quick
ws: m1-perception
status: complete
completed: 2026-06-02
parent_phase: 05-ws-book-prices
follow_up_of: 260602-diag-depth
files_modified:
  - src/polyarb/daemon/ws_consumer.py (+subscribe_candidates_payload / unsubscribe_candidates_payload — payload-only, no state mutation)
  - src/polyarb/observation/l2_candidate_refresh.py (on_snapshot_complete now sends WS payload after update_candidate_set)
  - tests/observation/test_l2_candidate_refresh.py (+3 RED→GREEN tests for the wiring contract)
---

# Quick Task SUMMARY: payload-only mid-connection subscribe/unsubscribe for L2 candidate diff

## Context (Phase 05 Wave 5 carry bug 3 — discovered via 260602-diag-depth)

The diagnostic env override (`POLYARB_L3_DEPTH_MIN_USD=0`) shipped earlier
this session let us bypass the `depth_yes_usd > 500` filter to see what was
actually in `l2_top_of_book`. The result was unexpected:

```
Even with threshold=0, L3 promoter: markets=0 tokens=0
```

Chain-walked to the real cause:

1. WS cold-start subscribes to 3 bootstrap asset_ids with `initial_dump=True`
2. Those 3 receive `book` frames → `_tob_row_from_frame` (SESSION 34 fix) fills `depth_yes_usd` / `depth_no_usd` for them
3. `on_snapshot_complete` then computes 61 candidates and calls `ws_consumer.update_candidate_set(61_ids)`
4. **But `update_candidate_set` only mutates `_candidate_set` in memory** — no WS payload sent
5. The live WS connection still streams frames for ONLY the original 3 asset_ids
6. New candidates (56 of them) NEVER receive `book` events → `depth_yes_usd` stays NULL forever
7. SQL `NULL > 0` is false → recipe matches zero rows → promoter outputs `markets=0`

This was bug 3, structurally distinct from bug 1 (NOT NULL violation) and the
hypothesized bug 2 (data skew). Bug 2 was a misdiagnosis; this is the real
unblocker for Wave 5 Task 2 (D-12 24h soak).

## Resolution

Two new payload-only methods on `WsConsumer`:

- `subscribe_candidates_payload(asset_ids)` — sends mid-conn `subscribe` JSON
  with `initial_dump=True`, no state mutation
- `unsubscribe_candidates_payload(asset_ids)` — sends `unsubscribe` JSON, no
  state mutation

`on_snapshot_complete` calls these after `update_candidate_set(...)` has
already mutated `_candidate_set` in memory.

### Why payload-only (not reusing `add_subscriptions` / `remove_subscriptions`)

`add_subscriptions` / `remove_subscriptions` were designed for the L3 promoter
path and own `_l3_active_set`. Using them from the L2 candidate refresh path
would corrupt the L3 active set with L2 candidate ids — caught explicitly by
`test_candidate_refresh_l3_protection.test_on_snapshot_complete_does_not_clobber_l3_active_set`:

```
AssertionError: L3 set must be UNTOUCHED;
got {'new_cand_2', 'l3_token_2', 'new_cand_1', 'l3_token_1'}
```

Two disjoint roles (`_candidate_set` for L2, `_l3_active_set` for L3) per
Pitfall 5 fix. The new methods are the L2 counterpart, kept symmetric to the
L3 ones but without the state mutation that's already handled by
`update_candidate_set`.

## Tests

3 new tests in `tests/observation/test_l2_candidate_refresh.py`:

- `test_on_snapshot_complete_calls_ws_add_subscriptions_for_added` — added
  diff → `subscribe_candidates_payload` awaited with sorted asset_ids
- `test_on_snapshot_complete_calls_ws_remove_subscriptions_for_removed` —
  removed diff → `unsubscribe_candidates_payload` awaited
- `test_on_snapshot_complete_no_ws_subscribe_calls_when_no_diff` — no diff
  → neither method awaited (no-op safety)

(Test method names kept their original add/remove framing because they
describe the contract at the call site; the impl-side names changed to
disambiguate from the L3-flow methods.)

## Regression

```
tests/observation/test_l2_candidate_refresh.py           (24 tests) ✓
tests/m1-perception/test_l3_promoter.py                  (15 tests) ✓
tests/storage/test_l2_supabase_mirror.py                 ✓
tests/m1-perception/test_l2_supabase_mirror_persist.py   ✓
tests/m1-perception/test_candidate_refresh_l3_protection.py ✓ (Pitfall 5 invariant)
tests/m1-perception/test_ws_consumer_dynamic_subscribe.py ✓ (L3 path unaffected)
```

61 tests green. `make planning-status` shows zero drift.

## Deployment plan

1. `git push origin main` (RED + GREEN + this SUMMARY)
2. `env -u FLY_API_TOKEN flyctl deploy --config fly-l2.toml --remote-only`
3. Wait one promote cycle (5 min). Expected:
   - `flyctl logs` shows `book` frames arriving for the 61 candidates (not
     just the original 3)
   - `l2_top_of_book` new rows have `depth_yes_usd` populated for markets
     that actually have YES-side bids
   - `/health l3:active_count > 0` for the first time
4. If `l3:active_count > 0` stably for 5+ min → start Wave 5 Task 2 (24h soak)
5. The diagnostic env (`POLYARB_L3_DEPTH_MIN_USD`) was already unset earlier
   this session so the baseline `> 500` filter is now the gate.

## Pre-existing bugs found / impact

This is the third bug in a chain that was hidden by **two layers of fail-soft
envelope**:

| Layer | Failure | What hid it |
|---|---|---|
| Bug 1 (`included_at_ts`) | NOT NULL violation on upsert | Mirror's `try/except` returns False, daemon doesn't crash. Hidden 5+ days. |
| Bug 3 (this) | `update_candidate_set` no WS payload | No exception raised — it was just **forgotten code**. No signal to surface. Hidden since Phase 05 Plan 02 D-11 refactor. |

This is the same lesson as the chain-truth discipline (Phase 03 Inj L2-2,
[[feedback_code-vs-chain-truth-2026-05]]): **silent forget-to-wire bugs need
an end-to-end test or `/health` chain-truth check**, not just unit tests on
each layer. The 3 RED tests added here serve as that end-to-end contract for
the candidate→WS path going forward.

Memory note: future phases that introduce a `_set` state with a corresponding
WS subscription should ALWAYS pair the in-memory mutator with a payload-
sender, OR add a `/health` chain-truth check (e.g.,
`ws:candidate_set_in_sync_with_subscribed_assets`) so a wiring gap is
immediately visible.
