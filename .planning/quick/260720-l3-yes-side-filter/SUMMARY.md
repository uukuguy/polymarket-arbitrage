---
quick_id: 260720-l3-yes-side-filter
ws: m1-perception
subsystem: observation
tags: [l3, yes-token, no-token, promoter, production]

requires:
  - quick: 260720-l3-latest-per-asset
    provides: One newest TOB row per asset

provides:
  - Authoritative Yes-side recipe input before the five-market limit
  - Protection against L3's own No subscriptions under-filling later ticks

affects:
  - Phase 05 Plan 06 strict 10-token stability prerequisite

tech-stack:
  added: []
  patterns:
    - "Resolve semantic identity before ranking/limiting time-series candidates"

key-files:
  modified:
    - src/polyarb/observation/l3_promote.py
    - tests/m1-perception/test_l3_promoter.py
    - docs/learning/11-L3-K线.md

key-decisions:
  - "Filter only wrong-side assets before the recipe; real incomplete Yes pairs still fail closed"
  - "Freeze on an empty identity projection rather than demote all active L3 tokens"

requirements-completed: []
completed: 2026-07-20
---

# Quick 260720: L3 Yes-Side Recipe Filter Summary

The promoter now resolves recent TOB assets against durable Yes-token identity
before the recipe limit, preventing L3's own No subscriptions from feeding back
into market selection.

## Evidence and Accomplishments

- Release 36 startup tick: 10/10; second tick: 8/10.
- Production dry-run identified No token `191083…` as the rejected pseudo-Yes
  row, exactly matching one promoted market's paired No identity.
- RED test reproduced a high-depth No row displacing the fifth valid Yes market.
- Identity resolution now precedes scanner evaluation; empty identity projection
  freezes state instead of causing a destructive demotion.
- Production-backed mutation-free dry-run returned 5 markets / 10 tokens under
  unchanged thresholds.

## Verification

- Focused promoter/dry-run suite: 21 passed.
- Ruff: passed.
- Full repository: 1433 passed, 1 skipped, 1 xfailed.
- Two-tick production proof runs after deployment before final closure.

## Production Boundary

No trading, funds, recipe threshold, secret, or config changed. The 24-hour soak
remains separate from the two-tick rollout prerequisite.

---
*Quick task: 260720-l3-yes-side-filter*
*Completed: 2026-07-20*
