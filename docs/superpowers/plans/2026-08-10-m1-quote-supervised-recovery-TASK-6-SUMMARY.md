# M1 Quote P1 Load-Amplification Containment — Task 6 Summary

## Delivered

The supervised Quote parent now rejects an already-stale durable Quote run
from its compact current-generation metadata before loading and rescanning the
full 40k-token projection. This prevents the 15-second feed hydrator from
turning an upstream Quote outage into repeated expensive SQLite/CPU work.

The default-off opportunity-first loop is now actually off: the parent starts
the focused active-master CLOB polling loop only when
`POLYARB_OPPORTUNITY_FIRST_WATCHER_ENABLED=true`. Global Quote certification
and its durable opportunity reconciliation remain enabled independently.

## Root-cause evidence

On Fly v304, process inspection found parent PID 662 producing small CLOB
`chunk 1/1` requests while an isolated full-universe Quote collection was in
progress. The parent loop was running despite the opportunity-first feature
flag being false. After Quote became stale, the parent hydrator also repeatedly
loaded the complete projection before discovering its age. Together these
competing loads correlated with `/healthz`, opportunity and console timeouts.

## Verification

- The stale-feed regression test was observed red first, then green: a stale
  metadata row raises `StaleQuoteRunError` without invoking the full projection
  loader.
- The default-off focused-polling topology regression test was observed red
  first, then green.
- `uv run pytest tests/daemon/test_quote_worker.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/m1-perception/test_quote_feed_health.py tests/perception/test_supervisor.py -q` — 179 passed.
- Ruff passed for changed files. `git diff --check` reported only the pre-existing
  unrelated `.superpowers/sdd/task-7-brief.md` missing final newline.

## Follow-up

Deploy this containment before judging a fresh Quote cycle. The upstream CLOB
timeout itself remains a P1 signal and must be diagnosed from bounded attempt
timings; this change removes the local amplification, not the alert condition.
