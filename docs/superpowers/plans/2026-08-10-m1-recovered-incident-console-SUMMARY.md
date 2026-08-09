# M1 Recovered Incident Console — Summary

Date: 2026-08-10

## Problem

The direct Fly incident console showed only open incidents. A correct verified
recovery removed the card, so an operator arriving after an outage had no
same-page record of the fault, its automated handling, or its recovery proof.

## Change

The console now retains a bounded 24-hour operator view for the two production
critical scopes it owns: `quote-collection` and `capacity`.

- It reads the existing bounded recent-incident endpoints, then the existing
  exact-ID lifecycle endpoint for each returned incident.
- Each recovered card exposes the original severity, automatic action, next
  operator action, failure reason, terminal recovery time and evidence.
- The card links to the exact lifecycle JSON. No new mutable state, control
  endpoint, or secondary incident store was introduced.
- Any unavailable/saturated recent or lifecycle read fails the whole console
  visibly as an operator-visibility fault; it never reports a false all-clear.

## Verification

- RED: console route test failed until the recovered-incident section and
  Quote recent endpoint contract existed.
- GREEN: focused direct-console, recent-incident and lifecycle tests pass.
- `uv run ruff check src/polyarb/http/perception.py
  tests/m1-perception/test_perception_http.py` passes.
- `uv run pytest tests/m1-perception/test_m1_manual_contract.py -q` and
  `make docs-m1-check` pass.

## Operator URL

`https://polyarb-l1.fly.dev/perception/console`
