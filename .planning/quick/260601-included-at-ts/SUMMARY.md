---
quick_id: 260601-included-at-ts
type: quick
ws: m1-perception
status: complete
completed: 2026-06-02
files_modified:
  - src/polyarb/observation/l2_candidate_refresh.py (add datetime import + stamp included_at_ts in upsert row dict)
  - tests/observation/test_l2_candidate_refresh.py (+1 RED→GREEN test for included_at_ts contract)
---

# Quick Task SUMMARY: stamp `included_at_ts` in l2_candidates upsert row dict

## Context (carry bug 1 — surfaced SESSION 34 EOD, fixed SESSION 35)

After Phase 05 v23 deploy to prod polyarb-l2, the promoter could not populate the L3 active set:

```
/health checks.l3:active_count = 0/10 (warn — under-filled, blocking D-12 24h soak)
```

Direct prod log inspection traced the chain to `l2_candidates` upserts failing every refresh cycle:

```
ERROR | polyarb.storage.l2_supabase_mirror:upsert_candidates:334
  l2-mirror upsert_candidates failed rows=59:
  {'message': 'null value in column "included_at_ts" of relation "l2_candidates"
   violates not-null constraint', 'code': '23502', ...}
```

The schema (`l2_candidates.included_at_ts`) is NOT NULL, but the caller in
`on_snapshot_complete` omitted the key. `_NARROW_CANDIDATE_COLUMNS` projection in
the mirror's `_project` helper therefore mapped it to `None` → Postgres rejected
every batch with 23502. Candidate set never repopulated, so the L3 promoter
filter (recent candidates only) found nothing to promote. This was bug 1 in the
SESSION 34 carry-bugs list; bug 2 (`depth_yes_usd` NULL skew) is a likely
derivative — once candidate diversity returns, mid-priced markets with two-sided
books should fill `depth_yes_usd` naturally.

## Resolution

`on_snapshot_complete` in `src/polyarb/observation/l2_candidate_refresh.py` now
stamps `included_at_ts = datetime.now(timezone.utc).isoformat()` once per
refresh cycle and writes it into every row of the upsert batch. This mirrors
`mark_candidates_removed`'s `now_iso` pattern: the caller is the semantic owner
of the timestamp ("when did inclusion happen?"), not the mirror layer.

Fix is one-line + one import. No schema change. No `removed_at_ts` change.

## Tests

1 new test in `tests/observation/test_l2_candidate_refresh.py`:

- `test_on_snapshot_complete_upsert_rows_include_included_at_ts` — asserts
  every row in the `upsert_candidates` argument has `included_at_ts` present,
  non-None, ISO-8601 tz-aware, and stamped within the wall-clock test window.

RED → GREEN: first run failed with the exact prod error pattern (missing key);
GREEN after adding the import + key + value.

## Regression

```
tests/observation/test_l2_candidate_refresh.py        (full file, 22 tests) ✓
tests/storage/test_l2_supabase_mirror.py              ✓
tests/m1-perception/test_l2_supabase_mirror_persist.py ✓
tests/m1-perception/test_candidate_refresh_l3_protection.py ✓
```

34 tests green. `make planning-status` shows zero drift.

## Why bug 1 is the ROOT CAUSE

Phase 04 D-07 / Alembic 004 (or 005) added `included_at_ts NOT NULL` to
`l2_candidates`. Writer side was not updated. Phase 04's chaos / acceptance
gates did not catch it because:

- mirror has fail-soft envelope (`return False`, log only — daemon does not crash)
- Phase 04 did not exercise the candidate→promoter path end-to-end
- `/health` did not check `l2_candidates` write success directly

Phase 05's L3 promoter is the first consumer that requires the candidate
table to be **actually populated**, so it was the first thing to fail visibly.

## Next step

1. Deploy: `env -u FLY_API_TOKEN flyctl deploy --config fly-l2.toml --remote-only`
2. ~5-10 min wait. Verify on prod:
   - Supabase: `SELECT COUNT(*) FROM l2_candidates WHERE included_at_ts > now() - interval '10 min'` → expect > 0
   - `/health` `l3:active_count` → expect > 0 (probably > 5 within 1-2 promoter cycles)
3. If `l3:active_count > 0` for 5+ min, open Wave 5 Task 2 (24h prod soak).
4. Re-check `depth_yes_usd` for new rows; if still NULL after 1-2 cycles, open
   the bug 2 quick task `260601-candidate-diversity`. If `depth_yes_usd > 0`
   appears for mid-priced markets, bug 2 is auto-healed and no follow-up
   needed.

## Pre-existing bugs found / impact

This fix closes the upper rung of the SESSION 34 carry-bugs chain. It is also
a clean example of a **chain-truth gap that fail-soft hid**: writer-side
envelope swallowed every batch failure for the entire post-v23 deploy window
without surfacing to `/health`, and the only downstream signal was an
under-filled L3 set. The chain-truth discipline rule (`CLAUDE.md` §chain-truth)
applies — a future hardening could let `/health` check
`l2_candidates.upsert_recent_success` instead of relying on consumer-side
silence as the failure detector.
