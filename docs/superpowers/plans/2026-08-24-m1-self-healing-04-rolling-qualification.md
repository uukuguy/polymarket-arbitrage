# M1 Rolling Qualification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace manually restarted formal runs with automatic rolling qualification epochs and immutable, reproducible certificates.

**Architecture:** A pure policy classifies contained versus breaking events. Migration 024 stores epochs and append-only certificates. A small service consumes durable runtime/incident/recovery/data-product facts; it never mutates jobs or Machines.

**Tech Stack:** Python 3.12, psycopg 3, PostgreSQL, SHA-256 canonical JSON, pytest, uv, Make.

## Global Constraints

- Execute after Plans 01-03.
- Preserve all old soak rows and failed runs; never relabel or delete them.
- Qualification binds role plus release/config policy identity, not eternal Machine IDs.
- A contained recovery may remain inside a certificate; correctness/fence/freshness/evidence breaches invalidate the epoch.
- Certificate verification is pure, deterministic, and independent of current mutable state.
- Use TDD and atomic commits. End with `05.6-204-SUMMARY.md` and clean `make planning-status`.

---

### Task 1: Pure qualification policy and virtual-time state machine

**Files:**
- Create: `src/polyarb/control_plane/qualification.py`
- Create: `tests/m1-perception/test_control_plane_qualification.py`

**Interfaces:**
- Produces: `QualificationState`, `QualificationFact`, `QualificationDecision`, `RollingQualificationPolicy.apply()`.

- [ ] **Step 1: Write failing policy tests**

```python
def test_contained_retry_keeps_epoch_accumulating() -> None:
    state = accumulating(start=NOW)
    result = policy.apply(state, contained_recovery(at=NOW + timedelta(hours=4)))
    assert result.state is QualificationState.ACCUMULATING
    assert result.invalidated_at is None

def test_integrity_or_expired_lease_invalidates_exact_epoch() -> None:
    result = policy.apply(accumulating(start=NOW), breaking_fact("lease.expired", at=NOW_PLUS_1H))
    assert result.state is QualificationState.INVALIDATED
    assert result.invalidated_at == NOW_PLUS_1H

def test_recovery_confirmation_opens_new_epoch_automatically() -> None:
    result = policy.apply(recovering(previous_epoch="epoch-a"), healthy_confirmation(at=NOW))
    assert result.state is QualificationState.ACCUMULATING
    assert result.started_at == NOW
```

Cover exact 24-hour boundary, maximum evidence gap, unresolved P1, repeated
signature budget, freshness breach, stale mutation, process replacement within
SLO, policy-version change, and count regression.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_qualification.py -q`

Expected: FAIL because qualification module does not exist.

- [ ] **Step 3: Implement states and policy**

```python
class QualificationState(StrEnum):
    ACCUMULATING = "accumulating"
    INVALIDATED = "invalidated"
    RECOVERING = "recovering"
    QUALIFIED = "qualified"

BREAKING_REASONS = frozenset({
    "lease.expired", "fence.mutated-stale", "integrity.conflict",
    "freshness.structure", "freshness.quote", "freshness.opportunity",
    "evidence.gap", "incident.p1-slo", "progress.regressed",
    "recovery.human-intervention",
})
```

The policy accepts ordered facts only. It rejects non-monotonic time and a
policy/release/config identity change inside one epoch. Reaching 86,400 seconds
returns `QUALIFIED` only after exact evidence coverage is proven.

- [ ] **Step 4: Verify and commit**

Run the qualification tests; expected PASS.

```bash
git add src/polyarb/control_plane/qualification.py tests/m1-perception/test_control_plane_qualification.py
git commit -m "feat(05.6-204): define rolling qualification policy"
```

### Task 2: Migration 024 and immutable certificate store

**Files:**
- Create: `alembic/versions/024_m1_rolling_qualification.py`
- Create: `tests/alembic/test_024.py`
- Create: `src/polyarb/control_plane/qualification_store.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Produces: epoch transition and certificate read/write methods.

- [ ] **Step 1: Write failing schema and store tests**

Assert revision `024` revises `023`, epoch transition compare-and-swap is
fenced by state/version, certificate UPDATE/DELETE raises, exact duplicate
certificate insertion is idempotent, and conflicting digest fails.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/alembic/test_024.py tests/m1-perception/test_control_plane_postgres.py -k qualification -q`

Expected: FAIL because revision 024 and store do not exist.

- [ ] **Step 3: Implement additive schema and canonical certificate**

Create `m1_qualification_epochs` and `m1_qualification_certificates` with the
fields in the approved design. Build certificate bytes using sorted compact
JSON and hash every identity, bound, count, SLO result, contained incident,
recovery action, evidence digest, and policy version:

```python
def canonical_certificate_bytes(payload: Mapping[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()

def certificate_digest(payload: Mapping[str, object]) -> str:
    return sha256(canonical_certificate_bytes(payload)).hexdigest()
```

Install append-only UPDATE/DELETE rejection on certificates.

- [ ] **Step 4: Verify and commit**

Run the tests from Step 2; expected PASS.

```bash
git add alembic/versions/024_m1_rolling_qualification.py tests/alembic/test_024.py src/polyarb/control_plane/qualification_store.py tests/m1-perception/test_control_plane_postgres.py
git commit -m "feat(05.6-204): persist rolling qualification certificates"
```

### Task 3: Qualification service and historical parity

**Files:**
- Create: `src/polyarb/control_plane/qualification_service.py`
- Create: `tests/m1-perception/test_control_plane_qualification_service.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:**
- Produces: `QualificationService.tick()`, `qualification-status`, `qualification-certificates`, `qualification-serve`.

- [ ] **Step 1: Write failing end-to-end virtual-time tests**

Feed runtime events, incidents, actions, and freshness snapshots through 26
virtual hours. Include one contained retry, one breaking expired lease,
recovery confirmation, and a later clean 24-hour interval. Assert the first
epoch is immutable invalidated and exactly one later certificate verifies.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_qualification_service.py -q`

Expected: FAIL because the service does not exist.

- [ ] **Step 3: Implement one bounded tick**

`tick(now)` locks the current epoch, reads only facts after its durable cursor,
applies them in timestamp/id order, persists each transition plus cursor, and
seals at most one certificate. A crash before commit replays the same facts; a
crash after commit resumes after the cursor. Add CLI commands:

```text
qualification-status --json
qualification-certificates --limit 20 --json
qualification-serve --enable --interval-seconds 30 --json
```

The status and certificate commands are read-only. The service receives a
qualification-scoped DSN and no R2, Fly mutation, Gamma, CLOB, or Telegram
credentials.

- [ ] **Step 4: Verify and commit**

Run qualification service, policy, CLI, and Postgres tests; expected PASS.

```bash
git add src/polyarb/control_plane/qualification_service.py tests/m1-perception/test_control_plane_qualification_service.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_cli.py
git commit -m "feat(05.6-204): evaluate rolling qualification continuously"
```

### Task 4: Makefile, certificate verifier, and closure

**Files:**
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-204-SUMMARY.md`

- [ ] **Step 1: Add failing target tests**

Assert status and certificate targets are read-only, and `qualification-serve`
requires `enable=1`.

- [ ] **Step 2: Add targets**

```make
## qualification-status: Read current rolling qualification progress and last breaker.
qualification-status:
	@uv run python -m polyarb.cli_control_plane qualification-status --json

## qualification-certificates: Read and reverify recent immutable qualification certificates.
qualification-certificates:
	@uv run python -m polyarb.cli_control_plane qualification-certificates --limit "$(or $(limit),20)" --json
```

- [ ] **Step 3: Run full gates**

Run all 024, qualification, CLI, Makefile, and Postgres tests plus Ruff.
Expected: PASS. Run virtual time with two independent replays; expected
identical certificate digest.

- [ ] **Step 4: Write SUMMARY and planning gate**

Record policy version, contained/breaking matrices, replay digest, commits, and
interfaces for Plans 05-06. Run `make planning-status`; expected no drift.
