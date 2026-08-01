# Task 5 Report — Atomic Structure Generation Readers

## Status

Implemented and locally verified. This task does **not** claim rollout, deployment,
production qualification, or trading authority.

## Delivered contract

- Added `structure_generation_read_mode` with the exact values
  `legacy | compare | generation`; default remains `legacy`.
- Added the `current_structure_markets` pointer-joined SQLite view.
- Added one shared `structure_read_transaction` / `StructureReadContext` which
  opens one transaction, resolves one identity, and supplies the matching market,
  membership, group-truth, event, tag, and issue tables.
- Generation reads authenticate pointer/publication/snapshot identity, component
  counts, market count, and the frozen generation hash before serving rows.
- Compare reads serve legacy data and expose a deterministic
  `StructureReadComparison` containing both snapshot IDs, market counts, universe
  hashes, source-truth hashes, and ordered mismatch reasons.
- Exact historical reads resolve the requested snapshot/generation and never
  consult the current pointer.
- Quote universe/run certification, focused membership, opportunity scanning,
  market-map/durable opportunity reads, and Supabase mirror projection now use one
  resolved identity per operation. Production daemon/CLI wiring propagates the
  configured mode.
- No wallet, signing, order placement, or trading authority was introduced.

## TDD evidence

RED was observed before implementation:

- `uv run pytest -q tests/m1-perception/test_structure_generation_readers.py`
  failed collection because `StructureGenerationReadError` and the shared read API
  did not exist.
- Consumer RED runs failed with unexpected keyword arguments for
  `NegRiskQuoteStore(... structure_generation_read_mode=...)`,
  `SqliteStructureMembershipReader`, `scan_neg_risk_buy_all`, and
  `_read_market_map`, plus a missing atomic mirror projection method.
- The first required combined run exposed 14 legacy-compatibility failures. Root
  cause analysis separated generation integrity gates from legacy-default behavior;
  count/hash fail-closed enforcement remains generation-only.

GREEN verification:

- Required five-suite command from the brief: **117 tests**, 0 failures, 0 errors,
  0 skips, 16.438 seconds.
- Store/config/publication/orchestrator/HTTP/mirror regressions: **272 tests**,
  0 failures, 0 errors, 1 existing skip, 27.558 seconds.
- Fresh combined completion run across both groups: **389 tests**, 0 failures,
  0 errors, 1 existing skip, 42.384 seconds.
- Changed-file Ruff: `All checks passed!`.
- `git diff --check`: clean.
- Direct-read audit across the listed production consumers found no current
  `markets`, membership, truth, or pointer query outside the shared context; the
  only textual match is an explanatory Supabase comment.

## Commit

`feat(m1): read one atomic structure generation` (the commit containing this report).

## Concerns / handoff

- Compare mismatch is exposed for Task 6 health integration; Task 5 intentionally
  does not alter `/health` policy.
- Supabase PostgREST remains a fail-soft, non-transactional remote adapter. This
  task guarantees that its local metadata and market projection originate from one
  resolved SQLite generation; it does not make the remote delete/insert atomic.
- Rollout remains at the default `legacy` mode. `compare` and `generation` require
  the later health/operations gates before any production enablement claim.
