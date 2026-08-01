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

## Review hardening follow-up

The review follow-up removed all full-universe work from the hot readers and
made comparison evidence durable:

- Legacy resolution now reads snapshot metadata only. It does not materialize
  markets, count structure rows, or compute hashes.
- Generation resolution is O(1): it authenticates the pointer, publication,
  snapshot, committed counts, validation hash, and certification marker from
  bounded metadata rows. Pointer publication remains atomic.
- Backfill creates a durable `structure_generation_comparison_receipts` row in a
  pinned SQLite read snapshot before pointer publication. Its two universe and
  source-truth hashes are computed with ordered cursor streaming outside the
  writer lock; compare mode then needs one receipt lookup and no universe scan.
- Missing, stale, identity-corrupt, or validation-hash-corrupt comparison receipts
  fail deterministically. Pointer/publication/snapshot corruption also fails
  closed before structure rows are served.
- Exact legacy Supabase mirror reads retain historical semantics, including
  invalid and non-`Structure` snapshots.

Additional RED tests first exposed the old hot-path calls, receipt absence/staleness,
legacy historical filtering, and metadata corruption. The final review gate ran
**402 tests**, with 0 failures, 0 errors, and 1 existing skip (62.921 seconds).
Changed-file Ruff, `git diff --check`, and `make planning-status` all passed; the
planning check reported no drift.

## Second re-review: bounded authenticated comparison

The second review moved comparison evidence into the same durable certification
chain as generation validation:

- Normal publications and backfills now traverse four keyset phases—legacy and
  generation universe, then legacy and generation rejections—before `ready`.
  Each invocation processes no more than `max_rows`; cursor, row count, digest
  state, phase, and checkpoint advance by CAS and survive store/process reopen.
- Canonical hash framing is unchanged. A small pure serializable SHA-256 state
  persists the eight FIPS state words, byte count, and at most 63 tail bytes.
  It matched `hashlib.sha256` for NIST vectors (including one million `a` bytes),
  every 0..130 split/reopen, tail boundaries, randomized partitions, empty input,
  and multi-block input. Malformed states fail closed and no prefix bytes accumulate.
- The exact current legacy snapshot is pinned before comparison and revalidated
  in both the read snapshot and writer transaction. Identity drift aborts sealing.
  A digest-bound immutable receipt is inserted in the same transaction that
  makes a normal publication ready; a migrated published pointer uses the same
  phases and atomically binds the digest without wedging generation reads.
- `receipt_digest` authenticates every receipt identity, count, universe/source
  hash, generation validation hash, and creation time. Readers recompute it,
  verify the pointer binding, and cross-check both snapshot market counts.
  Digest-sealed receipt UPDATE/DELETE operations are rejected.
- Literal pre-Task-5 pointers repair only when all four authentication fields are
  NULL and publication plus snapshot prove the frozen identity. A sealed receipt
  fills all four fields. Without one, initialization atomically fills the first
  three and creates active comparison provenance: generation remains usable,
  compare reports `comparison-receipt-missing`, and bounded backfill later binds
  the digest. Every fabricated partial, conflicting, or unverifiable state is
  unchanged by repeated init/backfill and remains fail-closed.
- Pointer publication verifies metadata and the sealed receipt only. SQL-trace
  tests prove it executes no `COUNT`, legacy membership scan, or generation market
  scan; the former one-shot backfill comparison helper is gone.

Second-review TDD observed RED for the missing SHA module/progress schema, absent
receipt repair, and the original unbounded backfill assumptions. Final focused
Task 3–5 verification passed **101 tests** with no failures, errors, or skips.
The full requested certification/backfill/pointer/schema/migration/hot-reader and
consumer regression passed **423 tests**, with 0 failures, 0 errors, and 1 existing
skip (48.941 seconds). Changed-file Ruff and `git diff --check` passed.

## Final narrow review: pointer state and retention

- Added the complete 14-case partial authentication matrix, including the
  digest-only fabricated state. Every case preserves the exact pointer row across
  repeated initialization and generic backfill; no field is opportunistically
  filled, generation raises, and compare reports a mismatch.
- Added the distinct all-four-NULL/no-receipt migration case. Initialization
  atomically records the three provable pointer facts plus durable active comparison
  provenance; only that provenance authorizes generation reads and bounded digest
  repair, while compare remains explicitly missing until sealing.
- Generic snapshot purge now excludes identities referenced by the current
  generation pointer, publications, comparison progress/receipts, published sync
  windows, and every generation component table during candidate selection.
  Tests prove an unrelated expired snapshot is deleted successfully while the
  authenticated generation plus exact legacy chain remains intact; replay deletes
  nothing and no sealed receipt mutation or FK rollback is used.
- Closed the candidate-selection TOCTOU window by acquiring `BEGIN IMMEDIATE`
  before keep-set and full evidence exclusion. A deterministic injection test
  reproduces the old FK rollback, then proves a competing generation-evidence
  insert is locked out until deletion commits, unrelated snapshots are still
  deleted, and replay is idempotent.
- Old generation reclamation deliberately remains out of generic retention. A
  dedicated bounded evidence-aware cleanup API must be implemented and exposed
  before production closure.
- Final narrow verification passed **175 focused tests** with no failures, errors,
  or skips. The complete requested regression passed **439 tests**, with 0
  failures, 0 errors, and 1 existing skip. Changed-file Ruff, `git diff --check`,
  and `make planning-status` (no drift) passed.
