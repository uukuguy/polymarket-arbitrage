# M1 Cross-Process Producer Arbitration Implementation Plan

> **For agentic workers:** Execute inline with test-first commits; this worktree
> must not use a second agent because the producer/storage boundary is shared.

**Goal:** Restore bounded, continuous Structure publication while preserving the
fresh supervised Quote feed and its 300-second SLA.

**Architecture:** Add one SQLite arbitration authority with a current lease and
bounded transition receipts. Quote and Structure use it before beginning their
respective write-heavy windows, so a parent-local `asyncio.Lock` is no longer
treated as a cross-process guarantee.

**Tech Stack:** Python 3.12, SQLite WAL, pytest, Fly L1.

## Global constraints

- No wallet, signing, orders, or manual production-data mutation.
- Quote must retain the 60s cadence, 180s hard child limit, 150s fetch limit
  and fail closed at 300s feed age.
- Structure ownership must be bounded to 45s and produce durable evidence for
  acquisition, defer, expiry recovery and release.
- All reads used by health/dashboard remain bounded and read-only.

---

### Task 1: Durable arbitration authority

**Files:**
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Create: `src/polyarb/daemon/producer_arbitration.py`
- Test: `tests/daemon/test_producer_arbitration.py`

**Interfaces:**
- Produces `ProducerArbitrator.acquire(owner, now_ms, lease_ms)`,
  `release(owner, lease_id, now_ms)`, and `snapshot(now_ms)`.
- `owner` is exactly `quote` or `structure`; acquire returns
  `ArbitrationDecision(granted, reason, lease_id, expires_at_ms)`.

- [ ] Write failing tests for quote/structure exclusion, expired-owner takeover,
  exact-owner release, and bounded receipt retention.
- [ ] Run `uv run pytest tests/daemon/test_producer_arbitration.py -q` and
  confirm the authority module is missing.
- [ ] Add a one-row current lease table plus bounded transition receipt table,
  serialized by `BEGIN IMMEDIATE`; reject malformed owners/durations and never
  overwrite a non-expired different owner.
- [ ] Run the Task 1 test file and changed-file Ruff; commit
  `feat(m1): add durable producer arbitration`.

### Task 2: Quote and Structure admission wiring

**Files:**
- Modify: `src/polyarb/daemon/quote_worker.py`
- Modify: `src/polyarb/daemon/scheduler.py`
- Modify: `src/polyarb/perception/worker_cli.py`
- Modify: `src/polyarb/daemon/main.py`
- Test: `tests/daemon/test_quote_worker.py`
- Test: `tests/m1-perception/test_scheduler.py`

**Interfaces:**
- Quote acquires `quote` before collection and releases after certification or
  terminal cleanup.
- Scheduler acquires `structure` before child admission; a denied admission
  creates its existing durable defer receipt with the arbitration reason.

- [ ] Write failing tests showing a parent scheduler does not treat its stale
  local Quote runtime as permanently due, a live quote lease defers Structure,
  and a released/expired quote lease admits one 45s Structure window.
- [ ] Run the named tests and observe failure under the current process-local
  runtime behavior.
- [ ] Wire both processes to the common arbitrator; retain existing in-process
  lock only as a local optimization, cap Structure ownership at 45s, and make
  Quote wait only until a live Structure lease expires before immediate retry.
- [ ] Run focused quote/scheduler suites and changed-file Ruff; commit
  `fix(m1): arbitrate quote and structure producers durably`.

### Task 3: Health, console and production qualification

**Files:**
- Modify: `src/polyarb/http/health.py`
- Modify: `src/polyarb/http/perception.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_quote_incidents.py`
- Modify: `.planning/JOURNAL.md`

**Interfaces:**
- `/healthz` exposes current arbitration owner, remaining lease and last
  transition without claiming a missing record is healthy.
- `/perception/console` renders arbitration defers/expiry as an operator action.

- [ ] Write failing health/console tests for a live Quote lease, stale lease,
  and verified Structure handoff.
- [ ] Run the tests and observe missing arbitration diagnostics.
- [ ] Add bounded read-only health and console projections; create or update a
  P1 incident when Structure starvation exceeds its configured publication SLA.
- [ ] Run focused health/console/Polywatch regressions, deploy exact HEAD,
  verify a new Structure publication followed by a fresh Quote run and empty
  open-incident state, then record evidence in JOURNAL.

## Plan self-review

- Task 1 owns storage authority; Task 2 is its only writer integration; Task 3
  owns outward visibility and production proof.
- The plan preserves Quote timing and makes no execution-capability change.
- No task relies on an undocumented API or an unbounded SQLite operation.
