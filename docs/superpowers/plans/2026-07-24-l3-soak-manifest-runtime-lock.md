# L3 Soak Manifest Runtime Lock Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make an immutable database-bound Phase 05.4 manifest enforce its five-market mapping only during `[T0,T24)`.

**Architecture:** Extend the binding event with exact lock bounds and mapping identity, expose one typed read through `L3EvidenceStore`, and let the promoter replace a changing dynamic proposal with the validated bound proposal. The sampler and verifier remain independent truth checks.

**Tech Stack:** Python 3.12, asyncio, asyncpg, pytest, existing Phase 05.4 evidence models and PostgreSQL tables.

## Global Constraints

- No new dependencies.
- Runtime role remains least-privilege SELECT/INSERT and never receives UPDATE, owner, service, or retention capability.
- No trading, order placement, or H-009 work.
- All mutations are append-only and every failure is fail closed.
- TDD red/green order is mandatory.

---

### Task 1: Canonical binding payload and typed lock read

**Files:**
- Modify: `src/polyarb/observation/l3_evidence.py`
- Modify: `src/polyarb/storage/l3_evidence_store.py`
- Modify: `scripts/l3_evidence.py`
- Test: `tests/m1-perception/test_l3_evidence_store.py`
- Test: `tests/m1-perception/test_l3_evidence_cli.py`

**Interfaces:**
- Produces: `SoakMappingLock(mapping_hash: str, t0: datetime, t24: datetime)`
- Produces: `L3EvidenceStore.fetch_active_soak_mapping_lock(*, boot_id: UUID, observed_at: datetime) -> SoakMappingLock | None`
- Changes binding detail to exact manifest hash, mapping hash, T0, and T24.

- [ ] **Step 1: Write failing CLI tests**

Assert the binding insert and read-back use:

```python
{
    "manifest_sha256": manifest.manifest_hash,
    "mapping_hash": manifest.mapping_hash,
    "t0": manifest.t0.isoformat().replace("+00:00", "Z"),
    "t24": manifest.t24.isoformat().replace("+00:00", "Z"),
}
```

Also assert any missing, added, or changed field fails exact binding validation.

- [ ] **Step 2: Run the CLI tests and confirm RED**

Run:

```bash
uv run pytest -q tests/m1-perception/test_l3_evidence_cli.py -k manifest_bind
```

Expected: failure because current detail contains only `manifest_sha256`.

- [ ] **Step 3: Write failing store tests**

Cover no rows, one active row, T0 inclusive, T24 exclusive, two overlapping
rows with the same mapping, two conflicting mapping hashes, malformed bounds,
and a binding whose `recorded_at >= t0`.

- [ ] **Step 4: Run store tests and confirm RED**

Run:

```bash
uv run pytest -q tests/m1-perception/test_l3_evidence_store.py -k soak_mapping_lock
```

Expected: failure because the method and DTO do not exist.

- [ ] **Step 5: Implement the binding and store boundary**

Add the frozen validated DTO, parameterized SELECT, strict JSON/timestamp/hash
validation, same-hash overlap rule, and typed redacted read failure. Update
`_binding_query_args`, binding INSERT, and `_validate_exact_binding` to use the
four-field canonical detail.

- [ ] **Step 6: Run Task 1 tests GREEN**

```bash
uv run pytest -q tests/m1-perception/test_l3_evidence_cli.py tests/m1-perception/test_l3_evidence_store.py
```

Expected: all pass.

### Task 2: Time-bounded promoter mapping enforcement

**Files:**
- Modify: `src/polyarb/observation/l3_promote.py`
- Test: `tests/m1-perception/test_l3_promoter.py`

**Interfaces:**
- Consumes: `fetch_active_soak_mapping_lock`
- Produces: `_locked_proposal(...) -> tuple[set[str], frozenset[str], tuple[dict[str, str], ...]]`

- [ ] **Step 1: Write failing promoter tests**

Use two opposite dynamic Top-5 fixtures. With no lock, assert the second set is
selected. With an active lock, seed the last-known identities/current desired
tokens for the first set and assert the terminal row keeps its mapping hash and
5/10/10/10 truth. Add missing-cache, mismatched-hash, and store-read-error cases
that each produce one non-success row with no control mutation.

- [ ] **Step 2: Run promoter tests and confirm RED**

```bash
uv run pytest -q tests/m1-perception/test_l3_promoter.py -k soak_mapping_lock
```

Expected: failure because promoter does not read or enforce a lock.

- [ ] **Step 3: Implement minimal lock enforcement**

Read the lock once after terminalization dependencies exist. For an active lock,
reconstruct only complete identities whose Yes/No tokens are in the current
desired or committed set, canonicalize by real market ID, require exactly
5/10 and exact hash, then route the result through existing control/mirror/
ledger code. Return one failed terminal draft on any lock read or reconstruction
failure. Leave the unbound branch byte-for-byte equivalent in behavior.

- [ ] **Step 4: Run Task 2 tests GREEN**

```bash
uv run pytest -q tests/m1-perception/test_l3_promoter.py
```

Expected: all pass.

### Task 3: Verification, release, and fresh formal attempt

**Files:**
- Modify after evidence exists: `.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-LOG.md`

**Interfaces:**
- Consumes the Task 1/2 implementation.
- Produces a new exact-SHA Fly boot and a new unique bound manifest attempt.

- [ ] **Step 1: Run all gates**

```bash
uv run pytest -q
uv run ruff check scripts/l3_evidence.py src/polyarb/observation/l3_evidence.py src/polyarb/observation/l3_promote.py src/polyarb/storage/l3_evidence_store.py tests/m1-perception/test_l3_evidence_cli.py tests/m1-perception/test_l3_evidence_store.py tests/m1-perception/test_l3_promoter.py
uv run python -m compileall -q src scripts tests/m1-perception
make docs-m1-check
make planning-status
```

Expected: pytest passes with only documented baseline xfail/skip/warnings,
changed-file Ruff is clean, and all other commands exit zero.

- [ ] **Step 2: Commit, push, and re-prove production boundaries**

Commit atomically, push `main`, require remote SHA equality, revision 007,
runtime-role PASS, retention-role PASS, runtime DSN present in Fly, and owner/
retention DSNs absent.

- [ ] **Step 3: Deploy and prove runtime lock before T0**

Trigger `deploy-l2.yml` at the exact SHA, cross-check GitHub/Fly/DB identity,
wait for two consecutive complete promoter rows and 12 continuous passing
samples, create a new O_EXCL manifest, bind it, and query the exact binding
detail before T0.

- [ ] **Step 4: Continue the existing immutable checkpoint plan**

Require the exact scheduled-T0 sample, then retain T0/T+6/T+12/T+18/T+24 and
run final verification without overwriting any artifact.

