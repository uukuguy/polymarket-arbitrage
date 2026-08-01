# M1 Candidate Lifecycle Queue Implementation Plan

> **For implementation:** Use `subagent-driven-development` when explicitly selected, or execute inline task-by-task with `test-driven-development` and `verification-before-completion`. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist and track every certified gross opportunity candidate without allowing lifecycle reconciliation or notification delivery to delay Quote feed publication or the next Quote attempt.

**Architecture:** The durable Quote-run certification transaction atomically enqueues an idempotent `(quote_run_id, universe_hash)` work item before the in-memory feed is published. A dedicated single consumer leases the oldest eligible item, rebuilds that exact historical certified projection, reconciles candidates in bounded chunks with stable opportunity IDs, and independently drains the durable notification outbox. Queue, cursor, terminal receipt, health, and incident evidence survive daemon restart; certified runs are never coalesced away.

**Tech Stack:** Python 3.12, asyncio, stdlib SQLite/WAL, Starlette, Typer, pytest, uv, Fly.io.

## Global Constraints

- The certified Quote feed is published before queue consumption and remains independently usable if lifecycle processing fails.
- Queue identity is exactly `(quote_run_id, universe_hash)`; mismatched or incomplete Quote runs fail closed.
- Candidate writes are bounded, idempotent, and restart-safe; a committed cursor is never advanced before its candidate transaction commits.
- Every complete certified Quote run reaches a terminal queue receipt; backlog policy may alert or apply admission backpressure but may not drop/supersede a run.
- Gross candidates remain `execution_status=not-verified`; no fee, slippage, fill, wallet, or trading claim is added.
- Quote age greater than 300 seconds remains an unconditional serving failure.
- Notification delivery is retryable and cannot mutate the underlying market observation.
- Every executable operator surface must have a documented Makefile target.

---

## File map

- `src/polyarb/storage/schemas.py`: reconciliation queue, checkpoint, and terminal receipt tables.
- `src/polyarb/routing/neg_risk_quote_store.py`: exact certified projection loader by run ID.
- `src/polyarb/routing/opportunity_ledger.py`: idempotent bounded candidate reconciliation and stable transition semantics.
- `src/polyarb/daemon/opportunity_watcher.py`: one bounded queue-consumption step and separate notification step.
- `src/polyarb/daemon/quote_worker.py`: enqueue after atomic feed publication; remove synchronous global reconciliation.
- `src/polyarb/daemon/main.py`: start and stop the lifecycle consumer independently.
- `src/polyarb/http/market_map.py`: status/history from durable lifecycle authority rather than stale market-map availability.
- `src/polyarb/http/arbitrage.py`: attach durable IDs only when lifecycle identity matches the served certified feed.
- `src/polyarb/http/health.py`: queue lag, lease, terminal receipt, lifecycle count, and notification backlog checks.
- `src/polyarb/snapshot/cli.py` or `src/polyarb/perception/worker_cli.py`: bounded local/production queue worker and status commands.
- `Makefile`, `docs/M1-市场感知平台使用手册.md`: operator surfaces and interpretation.

---

### Task 1: Add durable queue contracts and store operations

**Files:**
- Modify: `src/polyarb/storage/schemas.py`
- Create: `src/polyarb/routing/opportunity_reconciliation_queue.py`
- Create: `tests/m1-perception/test_opportunity_reconciliation_queue.py`
- Modify: `tests/m1-perception/test_schema_lockstep.py`

**Interfaces:**
- Produces:
  - `OpportunityReconciliationQueue.enqueue(quote_run_id, universe_hash, enqueued_at_ms) -> EnqueueResult`
  - `OpportunityReconciliationQueue.enqueue_in_transaction(con, quote_run_id, universe_hash, enqueued_at_ms) -> EnqueueResult`
  - `OpportunityReconciliationQueue.claim(owner, now_ms, lease_ms) -> ReconciliationClaim | None`
  - `OpportunityReconciliationQueue.checkpoint(claim, after_group_id, processed_count, now_ms) -> None`
  - `OpportunityReconciliationQueue.complete(claim, receipt, now_ms) -> None`
  - `OpportunityReconciliationQueue.fail(claim, error_kind, now_ms) -> None`
  - `OpportunityReconciliationQueue.status(now_ms) -> ReconciliationQueueStatus`

- [ ] **Step 1: Write RED queue tests**

Cover first enqueue, duplicate enqueue, FIFO claim ordering, claim lease,
expired-lease reclaim, wrong-owner rejection, cursor checkpoint, terminal receipt,
and restart continuation. Assert a transaction failure leaves both cursor and
processed count unchanged.

- [ ] **Step 2: Add exact queue DDL**

Create one row per Quote identity with statuses `queued`, `claimed`, `complete`,
or `failed`; unique `(quote_run_id, universe_hash)`; claim owner,
lease expiry, attempt count, `after_group_id`, processed count, last error, and
timestamps. Add an append-only terminal receipt table containing assessment
count, entered/changed/closed/unavailable counts, candidate-authority hash, and
receipt hash.

- [ ] **Step 3: Implement lossless enqueue and FIFO claim**

A duplicate returns the existing ID. Claim selects the oldest eligible certified
run by `(enqueued_at_ms, quote_run_id)`; no later run may supersede or delete an
earlier queued/claimed run. Add indexes for FIFO status/lease selection and make
queue depth/oldest age observable for backpressure and alerting.

- [ ] **Step 4: Implement lease-safe claim/checkpoint/terminal operations**

Every mutation verifies queue ID, identity, owner, current status, and unexpired
lease. `checkpoint` advances only to a lexicographically greater group ID.
`complete` appends the terminal receipt and changes status in one transaction.
`enqueue_in_transaction` performs the same idempotent insert on a caller-owned
SQLite transaction so Quote-run certification and queue visibility share one
commit; it must not begin or commit a nested transaction.

- [ ] **Step 5: Run and commit**

```bash
uv run pytest -q tests/m1-perception/test_opportunity_reconciliation_queue.py tests/m1-perception/test_schema_lockstep.py
git add src/polyarb/storage/schemas.py src/polyarb/routing/opportunity_reconciliation_queue.py tests/m1-perception/test_opportunity_reconciliation_queue.py tests/m1-perception/test_schema_lockstep.py
git commit -m "feat(m1): add durable opportunity reconciliation queue"
```

---

### Task 2: Load and verify one exact certified Quote projection

**Files:**
- Modify: `src/polyarb/routing/neg_risk_quote_store.py`
- Modify: `tests/routing/test_neg_risk_quote_store.py`

**Interfaces:**
- Consumes: queue `quote_run_id` and `universe_hash`.
- Produces: `NegRiskQuoteStore.complete_projection_for_run(run_id: int, expected_universe_hash: str) -> CompleteQuoteProjection`.

- [ ] **Step 1: Write exact-run RED tests**

Create two complete runs plus one collecting run. Assert exact run 1 loads even
when run 2 and a newer Structure generation exist; it resolves run 1's recorded
historical Structure generation. Wrong hash, failed/collecting status, missing
terminal rows, missing historical generation, or source-truth mismatch raises
`QuoteProjectionIntegrityError` without returning partial data.

- [ ] **Step 2: Extract the existing projection validator**

Reuse `_validate_complete_projection` and the same single read transaction used
by `latest_complete_projection`. Do not duplicate or weaken universe, quote,
membership, or count checks.

- [ ] **Step 3: Run and commit**

```bash
uv run pytest -q tests/routing/test_neg_risk_quote_store.py -k 'projection or exact_run'
git add src/polyarb/routing/neg_risk_quote_store.py tests/routing/test_neg_risk_quote_store.py
git commit -m "feat(m1): load exact certified quote runs"
```

---

### Task 3: Make lifecycle reconciliation bounded and idempotent

**Files:**
- Modify: `src/polyarb/routing/opportunity_ledger.py`
- Modify: `tests/routing/test_opportunity_ledger.py`
- Create: `tests/m1-perception/test_opportunity_reconciliation_worker.py`

**Interfaces:**
- Consumes: sorted `GroupAssessment` values for one exact run and queue cursor.
- Produces:
  - `OpportunityLedger.reconcile_global_batch(assessments, observed_at_ms, after_group_id, limit) -> ReconciliationBatch`
  - stable `opportunity_id` and transition kinds `entered`, `edge-changed`, `unchanged`, `closed`, `unavailable`, and `reappeared`.

- [ ] **Step 1: Write lifecycle transition tests**

Prove first detection creates one master and one `entered` observation; replay of
the same Quote/run is idempotent; a >=25 bps edge change records `edge-changed`;
no-edge records `closed`; the same membership returning later records
`reappeared` with the same stable opportunity ID; unavailable appends evidence
without claiming zero edge.

- [ ] **Step 2: Replace per-group transactions with one bounded batch**

Sort by `group_id`, take at most the requested limit after the exact cursor, and
perform master transition, observation append, and notification intent in one
transaction per batch. Add a unique observation identity over opportunity,
source, structure revision, quote run, and status so replay is a no-op.

- [ ] **Step 3: Return a deterministic receipt**

`ReconciliationBatch` reports last group ID, processed count, transition counts,
done flag, and a hash over ordered affected opportunity IDs and observations.
The queue checkpoint stores exactly that cursor/count after the ledger commit.

- [ ] **Step 4: Run and commit**

```bash
uv run pytest -q tests/routing/test_opportunity_ledger.py tests/m1-perception/test_opportunity_reconciliation_worker.py
git add src/polyarb/routing/opportunity_ledger.py tests/routing/test_opportunity_ledger.py tests/m1-perception/test_opportunity_reconciliation_worker.py
git commit -m "feat(m1): reconcile candidate lifecycles in bounded batches"
```

---

### Task 4: Build the independent reconciliation consumer

**Files:**
- Modify: `src/polyarb/daemon/opportunity_watcher.py`
- Modify: `src/polyarb/daemon/main.py`
- Modify: `src/polyarb/perception/worker_cli.py`
- Modify: `tests/daemon/test_opportunity_watcher.py`
- Modify: `tests/m1-perception/test_l1_quote_worker_wiring.py`
- Modify: `tests/m1-perception/test_daemon_shutdown.py`

**Interfaces:**
- Consumes: Tasks 1–3 queue, exact projection, scanner assessment, and batch ledger APIs.
- Produces:
  - `OpportunityWatcher.consume_reconciliation_once(owner, max_groups, lease_ms) -> ReconciliationOutcome`
  - independent `OpportunityWatcher.run_reconciliation(stop_event)` loop.

- [ ] **Step 1: Write restart and failure RED tests**

Cancel after one committed batch, instantiate a new consumer, and assert it
resumes after the stored group cursor. Inject projection corruption, ledger
failure, lease loss, and notification failure; verify each leaves retryable
durable evidence and never changes Quote runtime feed state.

- [ ] **Step 2: Implement one bounded consume step**

Claim, load exact projection, assess once, filter/sort after the queue cursor,
reconcile one bounded batch, checkpoint, renew lease, and either return
`checkpointed` or append the terminal receipt. Never call the latest-run loader.
Before claim, use the shared background-work admission gate and defer whenever
Quote is active or due. Release projection references after each batch and expose
defer reason/age; lifecycle work never owns the Quote-priority slot while idle.

- [ ] **Step 3: Run reconciliation and focused tracking as independent tasks**

Wire separate daemon tasks for global reconciliation and existing focused
tracking/delivery. Cancellation must settle the current batch, release no live
lease prematurely, and complete in the existing shutdown deadline.

- [ ] **Step 4: Add the bounded CLI**

Expose `run-reconciliation-once --max-groups 100` and JSON output containing
queue identity, cursor, counts, status, and error kind. The command processes no
notification transport unless explicitly invoked by the notification worker.

- [ ] **Step 5: Run and commit**

```bash
uv run pytest -q tests/daemon/test_opportunity_watcher.py tests/m1-perception/test_opportunity_reconciliation_worker.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/m1-perception/test_daemon_shutdown.py
git add src/polyarb/daemon/opportunity_watcher.py src/polyarb/daemon/main.py src/polyarb/perception/worker_cli.py tests/daemon/test_opportunity_watcher.py tests/m1-perception/test_opportunity_reconciliation_worker.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/m1-perception/test_daemon_shutdown.py
git commit -m "feat(m1): run lifecycle reconciliation independently"
```

---

### Task 5: Make Quote certification enqueue atomically and remove synchronous work

**Files:**
- Modify: `src/polyarb/routing/neg_risk_quote_store.py`
- Modify: `src/polyarb/daemon/quote_worker.py`
- Modify: `src/polyarb/daemon/main.py`
- Modify: `tests/routing/test_neg_risk_quote_store.py`
- Modify: `tests/m1-perception/test_opportunity_reconciliation_queue.py`
- Modify: `tests/m1-perception/test_l1_quote_worker_wiring.py`
- Modify: `tests/m1-perception/test_quote_feed_health.py`

**Interfaces:**
- Consumes: Task 1 queue enqueue and the Structure plan's `pipeline_active` boundary.
- Produces: a queue row in the same durable transaction that certifies a Quote run, followed by in-memory feed publication; no synchronous global reconciliation callback.

- [ ] **Step 1: Write certify-enqueue-publish ordering tests**

Inject failure before the Quote certification transaction commits and assert
neither a complete run nor queue item nor new in-memory feed is visible. Commit
certification and assert the matching queue row exists before `certified_feed()`
returns the new run. Duplicate publication remains idempotent. Assert the next
Quote cadence is not delayed by queue consumption or notification delivery.

- [ ] **Step 2: Enqueue in the durable certification transaction**

Extend the existing Quote-store terminal certification transaction to insert the
idempotent queue identity from the same certified metadata. The worker verifies
that identity when loading the complete projection, publishes the in-memory feed,
and calls `mark_success`; it performs no second queue write. Remove
`await opportunity_watcher.reconcile_global_projection(projection)` and the
`ReconcileGlobalProjection` callback from the Quote loop.

- [ ] **Step 3: Release full projection memory before maintenance**

After enqueue, drop full projection/run references and run the existing memory
release hook. Old-run cleanup is scheduled as independent bounded maintenance;
it cannot execute before the next Quote attempt when Quote is already due.

- [ ] **Step 4: Run and commit**

```bash
uv run pytest -q tests/routing/test_neg_risk_quote_store.py tests/m1-perception/test_opportunity_reconciliation_queue.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/m1-perception/test_quote_feed_health.py tests/m1-perception/test_arbitrage_opportunities_http.py
git add src/polyarb/routing/neg_risk_quote_store.py src/polyarb/daemon/quote_worker.py src/polyarb/daemon/main.py tests/routing/test_neg_risk_quote_store.py tests/m1-perception/test_opportunity_reconciliation_queue.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/m1-perception/test_quote_feed_health.py
git commit -m "fix(m1): decouple quote publication from lifecycle work"
```

---

### Task 6: Expose durable IDs, history, queue health, and alerts

**Files:**
- Modify: `src/polyarb/http/market_map.py`
- Modify: `src/polyarb/http/arbitrage.py`
- Modify: `src/polyarb/http/health.py`
- Modify: `scripts/polywatch/healthz_watcher.py`
- Modify: `src/polyarb/perception/worker_cli.py`
- Modify: `Makefile`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Modify: `tests/m1-perception/test_opportunity_watcher_http.py`
- Modify: `tests/m1-perception/test_arbitrage_opportunities_http.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_polywatch_healthz_watcher.py`
- Modify: `tests/m1-perception/test_makefile_contract.py`

**Interfaces:**
- Consumes: queue terminal receipts and existing lifecycle/notification tables.
- Produces: `make opportunity-reconciliation-status`, `make opportunity-history opportunity_id=<id>`, and component-specific Polywatch incidents.

- [ ] **Step 1: Remove market-map availability from watcher status**

`/opportunity-watch/status` reads queue status, current durable candidate count,
last terminal receipt, and notification backlog directly. A stale Structure may
make current candidate claims unavailable, but it cannot erase historical
masters or make exact-ID history unreadable.

- [ ] **Step 2: Attach IDs only on exact identity match**

The opportunity endpoint asks `durable_opportunity_ids` for the served
`source_snapshot_id`, `quote_run_id`, membership hash, and group ID. Matching
rows receive a non-null ID; a queue lag leaves ID null plus explicit
`lifecycle_status="pending"`, never a fabricated identifier.

- [ ] **Step 3: Add strict queue health**

Report oldest eligible queue age, claimed lease age/owner, last terminal run,
processed count, authority count, and notification backlog. Lag is warn before
its configured bound and fail afterward; Quote feed health remains independent.

- [ ] **Step 4: Add Polywatch component incidents**

Track reconciliation and notification delivery separately. Recovery requires a
new matching terminal receipt or delivery attempt; Quote recovery cannot close
either incident.

- [ ] **Step 5: Add Make/manual contracts and run gates**

```bash
uv run pytest -q tests/m1-perception/test_opportunity_watcher_http.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py tests/m1-perception/test_makefile_contract.py
make docs-m1-check
```

Expected: valid zero, pending lifecycle, unavailable lifecycle, and tracked
candidate are distinct response states.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/http/market_map.py src/polyarb/http/arbitrage.py src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py src/polyarb/perception/worker_cli.py Makefile docs/M1-市场感知平台使用手册.md tests/m1-perception/test_opportunity_watcher_http.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py tests/m1-perception/test_makefile_contract.py
git commit -m "feat(m1): expose durable candidate lifecycle health"
```

---

### Task 7: Deploy and prove lifecycle tracking without Quote regression

**Files:**
- Modify: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-02-SUMMARY.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/workstreams/m1-perception/ROADMAP.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Create: `docs/learning/46-gross-candidate-lifecycle.md`
- Modify: `docs/learning/00-INDEX.md`

**Interfaces:**
- Consumes: completed Structure generation plan and Tasks 1–6.
- Produces: final M1 production acceptance evidence.

- [ ] **Step 1: Run the complete gate**

```bash
make test-m1-perception
make docs-m1-check
make planning-status
git diff --check
```

Expected: all pass on one committed revision.

- [ ] **Step 2: Deploy the exact SHA and observe three Structure cycles**

Sample strict health and opportunity feed every ten seconds. Record the maximum
Quote age, every HTTP status, generation/snapshot/run identities, queue lag, and
terminal receipts. Any Quote age `>=300` or opportunity HTTP 503 fails the run.

- [ ] **Step 3: Prove real candidate lifecycle**

For at least one detected gross candidate, record its non-null opportunity ID,
first-seen observation, one later unchanged or changed observation, and exact-ID
history response. If the market naturally closes/no-edge, record that transition;
otherwise use a fixture-only local transition test and do not mutate production
market truth.

- [ ] **Step 4: Prove failure, retry, and component recovery**

Use the existing scoped fault-control mechanism to fail one lifecycle or
notification operation. Verify an incident opens, the certified Quote feed
remains HTTP 200, retry succeeds, a matching terminal receipt closes the
incident, and resident Polywatch retains no unresolved state.

- [ ] **Step 5: Close planning and teaching artifacts**

Update SUMMARY, STATE, ROADMAP, JOURNAL, architecture thread, manual, learning
index, and the learning chapter. Run `make planning-status` after the evidence
commit and require no drift.

- [ ] **Step 6: Commit evidence**

```bash
git add .planning docs/learning docs/M1-市场感知平台使用手册.md
git commit -m "docs(m1): qualify durable candidate lifecycle"
```
