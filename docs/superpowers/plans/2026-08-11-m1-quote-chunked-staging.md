# M1 Quote Chunked Staging Implementation Plan

> **For agentic workers:** Execute inline with test-driven development; the active worktree already isolates this production repair.

**Goal:** Persist a production-sized Quote run in bounded staging chunks while
leaving the current certified generation atomic and read-only to M2.

**Architecture:** `NegRiskQuoteStore` gains a chunked terminal-quote writer
that reuses the collecting run and validates each chunk against immutable run
legs.  The collector records durable chunk progress.  Existing `complete_run`
continues to be the only certification and pointer-switch boundary.

**Tech Stack:** Python 3.12, SQLite WAL, pytest, Fly.io.

## Global Constraints

- Do not change the 180-second Quote child hard limit.
- Do not serve collecting/failed staging rows to opportunity or M2 readers.
- Preserve exact source receipt, cardinality, digest, and pointer checks.
- Keep every writer transaction bounded and failure visible in Dashboard.

### Task 1: Bounded terminal staging API

**Files:**
- Modify: `src/polyarb/routing/neg_risk_quote_store.py`
- Test: `tests/routing/test_neg_risk_quote_store.py`

- [ ] Write a failing test that stages three quotes with `chunk_size=1`, asserts
  the prior current projection remains selected, and asserts no result is
  visible until the existing `complete_run` call.
- [ ] Run the node with `uv run pytest tests/routing/test_neg_risk_quote_store.py::<node> -q`; expect missing chunked API.
- [ ] Add `record_terminal_quotes_chunked(run_id, quotes, *, chunk_size=1000,
  on_chunk_committed=None)`, using one `BEGIN IMMEDIATE` per chunk and the
  same token-identity validation as the one-shot writer.
- [ ] Add a failing-chunk test proving a prior certified pointer survives and
  the collecting run cannot certify at an incomplete count.
- [ ] Run focused store tests; expect pass.

### Task 2: Child progress and production wiring

**Files:**
- Modify: `src/polyarb/routing/neg_risk_quote_collector.py`
- Test: `tests/routing/test_neg_risk_quote_collector.py`
- Test: `tests/daemon/test_quote_worker.py`

- [ ] Write a failing collector test that injects a multi-chunk store and
  asserts durable attempt phase timings include `persist_chunks` and
  `persisted_quotes` before terminal completion.
- [ ] Run the node; expect failure because the collector invokes the one-shot
  writer only once.
- [ ] Wire the chunked writer with a synchronous post-commit checkpoint
  callback; retain typed `QuotePersistenceTimeoutError` for a failed chunk.
- [ ] Run focused collector and worker regressions; expect pass.

### Task 3: Verification and production proof

**Files:**
- Modify: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-54-SUMMARY.md`

- [ ] Run Quote store/collector/worker plus perception dashboard tests, Ruff,
  `make docs-m1-check`, and `make planning-status`.
- [ ] Deploy exactly once and verify the release SHA, a fresh certified Quote
  run with chunk progress, a live opportunity response, and incident recovery
  history in `/perception/console`.
- [ ] Record only production facts in the plan summary and journal.
