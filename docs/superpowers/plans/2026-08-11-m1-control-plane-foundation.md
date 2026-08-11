# M1 Transactional Control Plane Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a production-usable Supabase control-plane foundation with fenced jobs, immutable attempts, authenticated checkpoints, incidents, and alert outbox receipts while the existing M1 workers continue running.

**Architecture:** Alembic migration `009` adds the additive Postgres authority. A small psycopg repository owns atomic claim/checkpoint/finalize operations with lease-epoch fencing and idempotency keys. A shadow bridge projects existing SQLite Structure/Quote evidence into the new control plane without changing current publication pointers, and a bounded read API exposes the durable operational state.

**Tech Stack:** Python 3.12, psycopg 3, Postgres/Supabase, Alembic, pytest, testcontainers, Starlette, Makefile.

## Global Constraints

- Existing SQLite/Fly workers remain active until shadow parity is proven.
- Postgres mutations are idempotent and fenced by `(job_key, lease_epoch)`.
- At-least-once execution must produce exactly-once durable effects.
- Alert intent is committed in the same transaction as its incident event.
- No new workflow framework or dependency is introduced.
- Every executable command is exposed through the Makefile.
- Production migration is additive and reversible; no existing table or pointer is deleted.

---

### Task 1: Add reversible control-plane schema

**Files:**
- Create: `alembic/versions/009_m1_transactional_control_plane.py`
- Create: `tests/alembic/test_009.py`
- Modify: `Makefile`

**Interfaces:**
- Produces tables `m1_jobs`, `m1_job_attempts`, `m1_checkpoint_receipts`, `m1_generation_manifests`, `m1_publication_pointers`, `m1_incidents`, `m1_incident_events`, `m1_alert_outbox`, and `m1_alert_deliveries`.
- `m1_jobs` owns lease fields directly; a separate mutable lease table is unnecessary for the foundation slice.

- [ ] **Step 1: Write the failing migration contract test**

```python
def test_009_defines_fenced_jobs_attempts_checkpoints_and_outbox() -> None:
    text = Path("alembic/versions/009_m1_transactional_control_plane.py").read_text()
    assert 'revision = "009"' in text
    assert 'down_revision = "008"' in text
    for table in (
        "m1_jobs", "m1_job_attempts", "m1_checkpoint_receipts",
        "m1_generation_manifests", "m1_publication_pointers",
        "m1_incidents", "m1_incident_events", "m1_alert_outbox",
        "m1_alert_deliveries",
    ):
        assert f'"{table}"' in text
    assert "lease_epoch" in text
    assert "idempotency_key" in text
```

- [ ] **Step 2: Run RED**

Run `uv run pytest tests/alembic/test_009.py -v`.

Expected: FAIL because migration `009` does not exist.

- [ ] **Step 3: Implement migration `009`**

Use imperative Alembic operations. Required job columns are `job_key TEXT PRIMARY KEY`, `job_type TEXT`, `input_identity TEXT`, `state TEXT`, `checkpoint_cursor TEXT`, `checkpoint_digest TEXT`, `lease_owner TEXT`, `lease_epoch BIGINT NOT NULL DEFAULT 0`, `lease_expires_at TIMESTAMPTZ`, `next_attempt_at TIMESTAMPTZ`, `attempt_count BIGINT`, `last_error_class TEXT`, `created_at TIMESTAMPTZ`, and `updated_at TIMESTAMPTZ`. Enforce states `runnable, leased, retryable, checkpointed, succeeded, quarantined`.

Attempts and receipts use UUID/text primary keys plus unique `idempotency_key`. Incident events reference incidents; alert outbox rows reference incident events and use unique `(incident_event_id, channel)`. Delivery rows reference outbox rows and use unique `(outbox_id, attempt_number)`. Add runnable-job index `(state, next_attempt_at, updated_at)` and lease-expiry index `(state, lease_expires_at)`.

Downgrade drops only these nine tables in reverse dependency order.

- [ ] **Step 4: Add Makefile verification entry**

Add `control-plane-migrate-test` which requires an explicit test DSN, performs `upgrade 009 → downgrade 008 → upgrade 009`, and exits 77 when the DSN is absent. Add it to `make help` and `.PHONY`.

- [ ] **Step 5: Verify GREEN and commit**

Run `uv run pytest tests/alembic/test_009.py tests/alembic/test_008.py -v` and `uv run ruff check alembic/versions/009_m1_transactional_control_plane.py tests/alembic/test_009.py`.

Commit `feat(05.6-69): add M1 control-plane schema` with matching `05.6-69-SUMMARY.md`.

### Task 2: Implement fenced job repository

**Files:**
- Create: `src/polyarb/control_plane/__init__.py`
- Create: `src/polyarb/control_plane/models.py`
- Create: `src/polyarb/control_plane/postgres.py`
- Create: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Produces immutable `JobLease`, `JobAttempt`, and `CheckpointReceipt` dataclasses.
- Produces `PostgresControlPlane.enqueue_job`, `claim_job`, `heartbeat`, `checkpoint`, `finish`, and `reclaim_expired`.
- All methods receive a caller-owned psycopg connection factory; no global client is created.

- [ ] **Step 1: Write failing repository tests using Postgres testcontainer**

```python
lease = repo.claim_job(
    worker_id="worker-a", job_types=("structure-normalize",),
    lease_seconds=30, now=clock.now,
)
assert lease.lease_epoch == 1
assert repo.claim_job(worker_id="worker-b", job_types=("structure-normalize",),
                      lease_seconds=30, now=clock.now) is None
```

Add tests proving: expired lease is reclaimed at epoch 2; epoch-1 heartbeat/checkpoint is rejected; duplicate checkpoint idempotency key returns the original receipt; crash before commit leaves cursor unchanged; retryable finish preserves checkpoint and sets `next_attempt_at`; quarantined job is never claimed.

- [ ] **Step 2: Run RED**

Run `uv run pytest tests/m1-perception/test_control_plane_postgres.py -v`.

Expected: FAIL because `polyarb.control_plane` does not exist.

- [ ] **Step 3: Implement domain models**

Define `JobState = Literal["runnable", "leased", "retryable", "checkpointed", "succeeded", "quarantined"]`. `JobLease` contains `job_key`, `job_type`, `input_identity`, `lease_owner`, `lease_epoch`, `lease_expires_at`, `checkpoint_cursor`, and `checkpoint_digest`. Validate non-empty identities and timezone-aware timestamps in `__post_init__`.

- [ ] **Step 4: Implement atomic claim and fencing**

`claim_job` executes one transaction using `SELECT ... FOR UPDATE SKIP LOCKED` over runnable/retryable/expired-leased jobs ordered by `next_attempt_at NULLS FIRST, updated_at, job_key`, then increments `lease_epoch`, writes owner/expiry/state, and creates an immutable running attempt. Every heartbeat/checkpoint/finalize update includes `WHERE job_key=%s AND lease_owner=%s AND lease_epoch=%s AND state='leased'`; zero affected rows raises `StaleLeaseError`.

- [ ] **Step 5: Implement idempotent checkpoint and incident/outbox transaction**

`checkpoint` inserts `m1_checkpoint_receipts` with `ON CONFLICT (idempotency_key) DO NOTHING`, authenticates any existing receipt fields, then advances the job cursor only under the fenced lease. `record_incident_event` inserts the incident event and one outbox row per channel in the same Postgres transaction.

- [ ] **Step 6: Verify and commit**

Run `uv run pytest tests/m1-perception/test_control_plane_postgres.py -v` and `uv run ruff check src/polyarb/control_plane tests/m1-perception/test_control_plane_postgres.py`.

Commit `feat(05.6-70): add fenced M1 job repository` with matching summary.

### Task 3: Add idempotent SQLite-to-control-plane shadow bridge

**Files:**
- Create: `src/polyarb/control_plane/shadow.py`
- Create: `src/polyarb/cli_control_plane.py`
- Create: `tests/m1-perception/test_control_plane_shadow.py`
- Modify: `Makefile`

**Interfaces:**
- Consumes existing SQLite snapshot attempts, Structure publication progress, Quote attempts, open incidents, and opportunity lifecycle evidence.
- Produces deterministic Postgres job/attempt/checkpoint/incident identities without changing either Structure or Quote publication pointer.
- CLI commands: `shadow-sync --json` and `status --json`.

- [ ] **Step 1: Write failing shadow idempotency test**

Seed SQLite with one Structure checkpoint, one failed Quote attempt, and one open incident. Run `shadow_sync` twice and assert Postgres row counts are identical after the second run, all idempotency keys are deterministic, and no `m1_publication_pointers` row is mutated.

- [ ] **Step 2: Run RED**

Run `uv run pytest tests/m1-perception/test_control_plane_shadow.py -v`.

Expected: FAIL because the bridge and CLI do not exist.

- [ ] **Step 3: Implement deterministic projection**

Use source identities `sqlite:snapshot-attempt:<id>`, `sqlite:structure-publication:<publication_id>:<cursor>`, `sqlite:quote-attempt:<id>`, and `sqlite:incident:<incident_id>:<sequence>`. Insert with immutable conflict authentication; a mismatched duplicate raises `ShadowIdentityConflict` and records no partial batch.

- [ ] **Step 4: Add Makefile commands**

Add `control-plane-shadow-sync` and `control-plane-status`. Both load `.env`, require `POLYARB_SUPABASE_DB_DSN`, expose concise JSON, and never print credentials. The shadow command performs no pointer switch.

- [ ] **Step 5: Verify and commit**

Run `uv run pytest tests/m1-perception/test_control_plane_shadow.py tests/m1-perception/test_make_perception_contract.py -v` and Ruff on new files.

Commit `feat(05.6-71): shadow M1 runtime into control plane` with matching summary.

### Task 4: Expose bounded control-plane operational read API

**Files:**
- Create: `src/polyarb/http/control_plane.py`
- Create: `tests/m1-perception/test_control_plane_http.py`
- Modify: `src/polyarb/http/app.py`
- Modify: `Makefile`
- Create: `docs/learning/64-M1事务控制面.md`

**Interfaces:**
- Produces `GET /perception/control-plane` with current job counts, oldest runnable age, expired leases, recent attempts/checkpoints, open incidents, pending alert outbox, and delivery status.
- Failure response is typed `{"status":"unavailable","reason":"control-plane-read-unavailable"}`; it never returns an empty healthy payload.

- [ ] **Step 1: Write failing HTTP contract tests**

Test available, empty-but-valid, dependency unavailable, auth/redaction, and bounded-limit cases. Stop/replace the data worker fixture and prove the endpoint still reads the independent Postgres fixture.

- [ ] **Step 2: Run RED**

Run `uv run pytest tests/m1-perception/test_control_plane_http.py -v`.

Expected: FAIL because the route does not exist.

- [ ] **Step 3: Implement bounded read projection and route**

Use one read-only transaction with a five-second statement timeout and fixed limits: 20 attempts, 20 incidents, 20 pending outbox rows. Return counts separately from samples. Register the route in the app without coupling it to SQLite health construction.

- [ ] **Step 4: Add operator entry and learning document**

Add `make perception-control-plane` using the existing authenticated perception curl pattern. The learning document includes the transaction mental model, fencing example, failure classes, code map, self-check questions, and FAQ.

- [ ] **Step 5: Verify foundation and production shadow**

Run all four focused test files, Alembic upgrade/downgrade roundtrip on a test Postgres, Ruff, `make planning-status`, then deploy once. In production run the additive migration, execute shadow sync twice, prove stable row counts/idempotency, read the control-plane endpoint while current workers continue, and verify no current pointer changed.

- [ ] **Step 6: Commit production evidence**

Commit `docs(05.6-72): record control-plane foundation evidence` with exact migration revision, release SHA, row counts, endpoint response, pointer non-mutation proof, and alert/outbox status.
