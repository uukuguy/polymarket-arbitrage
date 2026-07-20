---
quick_id: 260720-l3-latest-per-asset
ws: m1-perception
subsystem: observation
tags: [l3, promoter, time-series, deduplication, production]

requires:
  - quick: 260720-book-order-normalization
    provides: Correct production TOB spreads and top-10 depth

provides:
  - Newest-row-per-asset input contract for the L3 promotion recipe
  - Strict recipe limit semantics where five rows represent five markets

affects:
  - Phase 05 Plan 06 strict 5-market / 10-token production gate

tech-stack:
  added: []
  patterns:
    - "Newest-first API result collapsed at the ingestion boundary before selection"

key-files:
  modified:
    - src/polyarb/observation/l3_promote.py
    - tests/m1-perception/test_l3_promoter.py
    - docs/learning/11-L3-K线.md

key-decisions:
  - "Deduplicate before recipe evaluation; do not weaken recipe thresholds or raise its limit"
  - "Retain the first row because the PostgREST query explicitly orders ts descending"

requirements-completed: []
completed: 2026-07-20
---

# Quick 260720: L3 Latest Row Per Asset Summary

The L3 promoter now evaluates one newest TOB snapshot per asset, so its locked
five-row recipe limit maps to five distinct markets.

## Evidence and Accomplishments

- Production-backed dry-run before the repair: 5 qualifying rows collapsed to
  3 markets / 6 tokens, with duplicate-pair warnings for the repeated asset.
- RED test proved `_fetch_latest_tob_rows_from_supabase` returned both snapshots
  of asset `a`.
- The fetch boundary now keeps only the first non-empty row per asset from the
  explicitly newest-first PostgREST result.
- Production-backed mutation-free dry-run after the repair: exactly 5 markets /
  10 tokens proposed under unchanged spread and depth thresholds.

## Verification

- Focused L3 promoter/dry-run tests: 20 passed.
- Ruff and `git diff --check`: passed.
- Full repository: 1432 passed, 1 skipped, 1 xfailed.
- L2 release 36: exact `10/10`, promote age 18.0s, book-write age 1.3s.
- Database after release: five promoted Yes markets, ten token identities, 200
  book-level rows over eight active tokens at the immediate read.
- That startup proof was later bounded: release 36's second tick fell to 8/10
  because a newly subscribed No row entered the Yes-market recipe. Quick task
  `260720-l3-yes-side-filter` repaired and re-verified the feedback loop.

## Production Boundary

No recipe threshold, trade path, funds, or exchange order changed. Production
image digest is `sha256:708d327fd0069e77b0aa32834fde0657ea3e1208b7e6a6b372101316453e926e`;
the 24-hour soak remains separate from this immediate rollout proof.

---
*Quick task: 260720-l3-latest-per-asset*
*Completed: 2026-07-20*
