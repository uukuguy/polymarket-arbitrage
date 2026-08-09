# M1 Capacity Episode Proof — Summary

Date: 2026-08-10

## Production fault

After capacity controller activation, normal measurements were recorded but
the old capacity incident could not verify. The controller refreshed its
`recovering` lifecycle while pressure continued, and the strict proof compared
the positive reclaim receipt to that most-recent refresh instead of the same
incident episode's detection time.

## Fix

Capacity verification now requires a positive `capacity_reclaim_receipts` row
between the incident's initial detected event and the current verification, plus
current runtime `normal` and matching receipt pointer. A later lifecycle refresh
cannot erase evidence for the same episode.

## Verification

- RED: a positive receipt followed by a later pressure refresh could not close
  after normal measurement.
- GREEN: the repeated-refresh episode test, existing capacity lifecycle and
  controller tests, plus perception diagnostic test pass (10 tests); Ruff and
  diff checks pass.
