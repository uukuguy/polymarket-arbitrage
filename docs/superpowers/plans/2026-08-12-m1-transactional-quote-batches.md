# M1 Transactional Quote Batches Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the one-child whole-universe Quote persistence boundary with fenced, checkpointed Quote batches and one certified terminal publication.

**Architecture:** An immutable Structure receipt and universe digest define a Quote generation. Postgres owns deterministic batch jobs and fenced receipts. The last certified Quote pointer remains scanner-visible until a certifier has authenticated every fresh batch receipt and atomically moves the pointer.

**Tech Stack:** Python 3.12, psycopg 3, Supabase Postgres, SQLite staging/cache, existing CLOB client, pytest/testcontainers, Starlette, Makefile.

## Global Constraints

- No partial generation changes `neg_risk_quote_current_generation` or serves through the opportunity feed.
- A batch key is `quote:<structure-receipt-digest>:batch:<ordinal>` and its input identity includes the universe hash and token-range digest.
- Each admitted batch persists its exact ordered token ids and their frozen market/condition/event/slug membership mapping in the control plane; a replacement worker must load that immutable input rather than reconstructing it from SQLite or a newer Structure pointer.
- Every batch writes canonical JSONL to a content-addressed R2 key and verifies object length plus SHA-256 metadata with `HEAD` before recording the fenced receipt. The terminal manifest hashes ordered receipt artifact keys/digests as well as quote digests.
- Every effect is fenced by `(job_key, lease_owner, lease_epoch)` and an idempotency key.
- A retry uses its original immutable token range and never reads a newer Structure pointer.
- SQLite remains comparison-only until shadow parity and a separately authorized pointer switch.
- No wallet, signing, order, or balance capability is added.

### Task 1: Admit and certify fenced Quote generations

**Files:**
- Modify: `alembic/versions/009_m1_transactional_control_plane.py`
- Modify: `src/polyarb/control_plane/models.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/alembic/test_009.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Create: `tests/m1-perception/test_transactional_quote_publication.py`

**Interfaces:**
- `QuoteBatchSpec.from_tokens(structure_receipt_digest, universe_hash, ordinal, token_ids)` returns immutable sorted token identities and a range digest.
- `enqueue_quote_generation(...)` creates deterministic `quote-batch` jobs and one blocked `quote-certify` job.
- `record_quote_batch(lease, ...)` commits a fenced receipt and checkpoint atomically.
- `certify_quote_generation(lease, generation_key, now)` moves the Quote pointer only after full coverage/freshness verification.

- [ ] **Step 1: Write failing admission and no-partial-publication tests**

```python
def test_quote_generation_admission_is_deterministic(control_plane, now):
    batches = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64, universe_hash="b" * 64,
        token_ids=("t3", "t1", "t2"), batch_size=2, now=now,
    )
    assert [item.token_ids for item in batches] == [("t1", "t2"), ("t3",)]

def test_incomplete_generation_cannot_switch_quote_pointer(control_plane, now):
    batches = _three_quote_batches(control_plane, now)
    _record_one_fenced_receipt(control_plane, batches[0], now)
    with pytest.raises(IncompleteQuoteGenerationError):
        control_plane.certify_quote_generation(_claim_certifier(control_plane, now), batches[0].generation_key, now)
    assert _current_quote_pointer(control_plane) is None
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_quote_publication.py -k 'quote_generation or incomplete_generation' -v`

Expected: FAIL because Quote generation contracts, batch receipts, and certification do not exist.

- [ ] **Step 3: Implement the smallest fenced data model**

Revision 009 gets `m1_quote_batch_receipts`, keyed by job key, containing Structure receipt, universe hash, range digest, quote digest, response count, and timestamp. Admission sorts/deduplicates tokens before batching. Receipt insertion authenticates a repeat receipt, advances its existing fenced job checkpoint in the same transaction, and rejects stale leases. Certification requires exactly one fresh receipt per expected range, creates an immutable Quote generation manifest, and compare-and-swaps the Quote pointer. Missing ranges make the certification job retryable without a pointer write.

- [ ] **Step 4: Run GREEN**

Run: `uv run pytest tests/alembic/test_009.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_quote_publication.py -k 'quote or publication' -v`

Expected: PASS for deterministic admission, stale-writer rejection, receipt idempotency, incomplete rejection, complete certification, and idempotent publication.

- [ ] **Step 5: Commit**

```bash
git add alembic/versions/009_m1_transactional_control_plane.py src/polyarb/control_plane/models.py src/polyarb/control_plane/postgres.py tests/alembic/test_009.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_quote_publication.py
git commit -m "feat(05.6): add fenced transactional quote generations"
```

### Task 2: Run one fenced batch at a time and expose its state

**Files:**
- Create: `src/polyarb/control_plane/quote_worker.py`
- Modify: `src/polyarb/config.py`
- Modify: `src/polyarb/daemon/main.py`
- Modify: `src/polyarb/http/control_plane.py`
- Modify: `Makefile`
- Create: `tests/m1-perception/test_transactional_quote_worker.py`
- Create: `tests/m1-perception/test_transactional_quote_operator.py`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Create: `docs/learning/NN-事务型Quote批处理.md`
- Modify: `docs/learning/00-INDEX.md`

**Interfaces:**
- `TransactionalQuoteWorker.run_once() -> bool` claims one `quote-batch` or `quote-certify` job.
- `make quote-control-plane-once` requires explicit DSN and enablement; comparison mode does not switch the legacy scanner pointer.
- `/perception/control-plane` returns retryable batch count, retry age, certification state, and durable pointer identity without SQLite reads.

- [ ] **Step 1: Write failing worker and operator tests**

```python
async def test_failed_batch_retries_without_publishing_partial_generation():
    worker = _worker_with_three_batches(fail_token="t3")
    assert await worker.run_once() is True
    assert await worker.run_once() is True
    assert _current_quote_pointer(worker.control_plane) is None
    worker.reader = _successful_reader()
    assert await worker.run_once() is True
    assert await worker.run_once() is True
    assert _current_quote_pointer(worker.control_plane) is not None

def test_operator_view_exposes_retryable_quote_batch_without_sqlite(client):
    _seed_retryable_quote_batch()
    response = client.get("/perception/control-plane", headers=_auth_headers())
    assert response.status_code == 200
    assert response.json()["quote"]["retryable_batches"] == 1
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/m1-perception/test_transactional_quote_worker.py tests/m1-perception/test_transactional_quote_operator.py -v`

Expected: FAIL because no transactional Quote worker or Quote operator projection exists.

- [ ] **Step 3: Implement bounded execution and visibility**

The worker claims one Postgres job, fetches only its `QuoteBatchSpec.token_ids`, heartbeats during CLOB I/O, and writes an ordered quote digest receipt. Transient CLOB/429/5xx/timeout failures finish only that job retryably with bounded backoff. The certifier uses Task 1 and does not read/write SQLite's current Quote pointer. Configuration is disabled by default. Extend the existing bounded Postgres snapshot with Quote batch counts and current pointer identity; add Make targets and the Chinese learning/runbook material.

- [ ] **Step 4: Run GREEN and local qualification**

```bash
uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_quote_publication.py tests/m1-perception/test_transactional_quote_worker.py tests/m1-perception/test_transactional_quote_operator.py -v
uv run ruff check src/polyarb/control_plane src/polyarb/config.py src/polyarb/daemon/main.py tests/m1-perception
make docs-m1-check
make planning-status
git diff --check
```

Expected: all commands exit zero; this is local evidence only and does not authorize production mutation.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/control_plane/quote_worker.py src/polyarb/config.py src/polyarb/daemon/main.py src/polyarb/http/control_plane.py Makefile tests/m1-perception/test_transactional_quote_worker.py tests/m1-perception/test_transactional_quote_operator.py docs/M1-市场感知平台使用手册.md docs/learning/00-INDEX.md docs/learning/NN-事务型Quote批处理.md
git commit -m "feat(05.6): execute transactional quote batches"
```

## Separately Authorized Production Acceptance

1. Apply Alembic 009 to the designated control-plane authority.
2. Run shadow sync twice and prove stable rows with zero pointer mutations.
3. Run comparison mode for three certified Structure identities and compare coverage, ordered quote digest, and response count with the legacy collector.
4. Authorize a reversible pointer switch and prove pointer-only rollback.
5. Kill one batch worker; prove fenced takeover, no duplicate receipt, continued prior feed, incident/outbox visibility, and later certified recovery.
6. Observe Structure, Quote, opportunity feed, control-plane reads, Dashboard, Polywatch, and Telegram throughout the qualification window before declaring M1 production acceptance.
