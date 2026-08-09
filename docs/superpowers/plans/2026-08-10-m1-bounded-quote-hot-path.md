# M1 Bounded Quote Hot Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use inline test-driven implementation; each task must remain independently verifiable.

**Goal:** Restore a continuously certifiable 39k-token M1 Quote feed without unbounded SQLite history writes or silent P1 notification ambiguity.

**Architecture:** Quote attempts retain small durable phase receipts.  Failed runs shed heavy legs/quotes immediately after terminalization; the published Feed retains only bounded authenticated generations.  A later online SQLite copy compacts reclaimed pages, but never occurs on the mounted live database.

**Tech Stack:** Python 3.12, SQLite WAL, existing `NegRiskQuoteStore`, pytest, Fly.io, R2.

## Global Constraints

- Never lengthen the 120-second child limit or freshness SLA.
- Never present stale/unavailable feed as zero opportunity.
- Preserve authenticated source and quote receipt evidence for every published Feed.
- Failed runs retain diagnosis and timing receipt, not 39k-row payload copies.
- Every production operation is bounded and has health/Dashboard evidence.

---

### Task 1: Failed-run heavy-payload reclamation

**Files:**
- Modify: `src/polyarb/routing/neg_risk_quote_store.py`
- Modify: `src/polyarb/daemon/quote_worker.py`
- Test: `tests/routing/test_neg_risk_quote_store.py`
- Test: `tests/daemon/test_quote_worker.py`

**Interfaces:**
- Produces `NegRiskQuoteStore.reclaim_terminal_failed_payloads(max_runs: int) -> int`.
- Called only after a run is terminal `failed`; the attempt receipt retains `quote_run_identity` and phase/failure fields.

- [ ] Write a failing test that creates a failed run with legs/quotes and asserts one bounded reclaim removes payload rows while retaining the failed metadata and attempt identity.
- [ ] Run `uv run pytest tests/routing/test_neg_risk_quote_store.py::test_failed_run_reclaim_keeps_diagnosis -q`; expect failure because the method does not exist.
- [ ] Implement one `BEGIN IMMEDIATE` transaction that selects only failed, non-collecting runs older than the newest failed receipt, detaches attempt FK identity, deletes quote/leg/source receipt payload, and keeps the run metadata.
- [ ] Write a failing worker test that makes a child failure terminal and asserts bounded reclaim is scheduled fail-soft before the next retry.
- [ ] Run focused store/worker tests; expect pass.
- [ ] Commit `fix(m1): reclaim failed quote payloads between retries`.

### Task 2: Current authenticated Feed generation

**Files:**
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/routing/neg_risk_quote_store.py`
- Modify: `src/polyarb/routing/neg_risk_quote_collector.py`
- Modify: `src/polyarb/routing/opportunity_scanner.py`
- Test: `tests/routing/test_neg_risk_quote_store.py`
- Test: `tests/routing/test_neg_risk_quote_collector.py`
- Test: `tests/routing/test_opportunity_scanner.py`

**Interfaces:**
- Produces a bounded staging/current generation selected by an atomic pointer.
- `latest_complete_projection()` reads only the pointer-selected complete generation.

- [ ] Write a failing test proving an incomplete staging generation is invisible to scanner and a previous complete generation remains readable.
- [ ] Run its focused pytest node; expect failure because no current-generation pointer exists.
- [ ] Add generation pointer and staging rows with immutable receipt digest; write staged payload in bounded chunks and switch pointer only after exact leg/quote cardinality and digest validation.
- [ ] Write a failing test proving a successful switch exposes the new generation atomically and reclamation cannot remove the selected generation.
- [ ] Run quote-store, collector, scanner suites; expect pass.
- [ ] Commit `feat(m1): publish bounded current quote generation`.

### Task 3: Operator truth and deployment proof

**Files:**
- Modify: `scripts/polywatch/healthz_watcher.py`
- Modify: `tests/m1-perception/test_polywatch_healthz_watcher.py`
- Modify: `docs/learning/` index and an M1 storage lesson

- [ ] Write a failing test asserting a P1 duplicate-suppression log reports its effective 300-second reminder rather than the P2/global 1800-second setting.
- [ ] Run the test; expect failure against the current misleading log.
- [ ] Render the effective interval in the watcher log and preserve existing notification state semantics.
- [ ] Run focused watcher tests plus `make planning-status`; expect pass/OK.
- [ ] Deploy directly, observe one certified Feed completion, strict health pass, Dashboard/API incident recovery, and capacity non-growth evidence.
- [ ] Commit `fix(m1): expose effective P1 reminder cadence`.

## Verification

1. Repeated failed collection does not grow heavy quote payload count after its bounded diagnostic retention.
2. An incomplete generation is never scanner-visible; pointer switch is atomic.
3. A 39,748-token certified run completes inside freshness/SLA without blocking `/healthz`.
4. P1 health, Telegram, incident API, and Dashboard agree on incident/recovery identity.
5. Physical compaction is performed only through the separately approved offline-volume replacement protocol.
