# M1 Structure Comparison Authentication Implementation Plan

> **For agentic workers:** implement inline in this worktree with strict TDD; the
> review requires one final atomic commit rather than per-task commits.

**Goal:** Make every authenticated ready Structure generation own a bounded,
digest-bound comparison receipt while preserving the existing canonical hashes.

**Architecture:** A pure serializable SHA-256 state feeds the unchanged canonical
JSON byte stream. A durable comparison-progress row pins legacy identity and
advances four keyset phases with CAS. Receipt sealing precedes `ready`; publication
and hot reads verify metadata only. Schema bootstrap repairs only provably valid
pre-Task-5 pointers.

**Tech Stack:** Python 3.12, SQLite, hashlib as the canonical oracle, pytest, uv.

## Global Constraints

- One invocation reads at most `max_rows` comparison rows and respects its elapsed deadline.
- Pointer switch and hot readers perform no full-table count or hash scan.
- Canonical universe/source-truth byte framing and SHA-256 output remain unchanged.
- Persist only eight SHA-256 words, byte count, and a tail of at most 63 bytes.
- No pickle, OpenSSL internal state, new dependency, or alternate hash algorithm.
- No rollout, deployment, health-policy, wallet, signing, or order-placement change.

---

### Task 1: Serializable canonical SHA-256 state

**Files:**
- Create: `src/polyarb/storage/serializable_sha256.py`
- Create: `tests/storage/test_serializable_sha256.py`

**Interfaces:**
- Produces: `SerializableSHA256.new()`, `update(bytes)`, `hexdigest()`,
  `to_json()`, and `from_json(str)`.

- [ ] Write failing tests for NIST empty/`abc`/multi-block vectors, equality to
  `hashlib.sha256` at every 0..129 byte split, randomized multipart updates,
  serialize/reopen at each 0..65 byte tail boundary, malformed words/count/tail,
  and serialized tail length <= 63.
- [ ] Run `uv run pytest -q tests/storage/test_serializable_sha256.py`; expect
  import failure because the module does not exist.
- [ ] Implement standard FIPS 180-4 SHA-256 compression with state JSON shaped as
  `{"words":[8 unsigned ints],"byte_count":N,"tail_hex":"..."}`; `hexdigest`
  finalizes a copy so the live state remains resumable.
- [ ] Re-run the focused suite and require exact `hashlib.sha256` equality.

### Task 2: Digest-bound immutable receipt and migration repair

**Files:**
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_schema_lockstep.py`
- Modify: `tests/m1-perception/test_structure_generation_readers.py`
- Modify: `tests/m1-perception/test_sqlite_store_migration.py`

**Interfaces:**
- Produces: `_comparison_receipt_digest(fields) -> str`, nullable pointer
  `comparison_receipt_digest`, receipt `receipt_digest`, and idempotent
  `_repair_current_structure_generation_authentication(con)`.

- [ ] Write RED tests creating literal pre-Task-5 pointer tables/data, then prove
  all four NULL auth fields repair atomically and repeatedly. With no receipt,
  prove the first three fields plus active comparison provenance are atomic,
  generation remains usable, compare reports `comparison-receipt-missing`, and
  bounded backfill seals the receipt. Exercise every fabricated mixed NULL/non-NULL
  combination and prove init/backfill preserve exact row equality while generation
  and compare remain fail-closed.
- [ ] Write RED tests proving receipt digest tamper and identity swap are reported,
  both-side false count/hash mutation is rejected or detected, and sealed UPDATE /
  DELETE raises `sqlite3.IntegrityError`.
- [ ] Add columns, progress table, receipt immutability triggers, canonical receipt
  digest, metadata cross-checks, and repair calls in both schema-init paths.
- [ ] Run the three focused schema/reader/migration suites until GREEN.

### Task 3: Bounded comparison certification and unified sealing

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_structure_generation_publication.py`
- Modify: `tests/m1-perception/test_structure_generation_readers.py`

**Interfaces:**
- Produces: comparison phases `legacy-universe`, `generation-universe`,
  `legacy-rejections`, `generation-rejections`; a progress row keyed by
  `publication_id`; and a shared bounded runner used by normal and backfill flows.

- [ ] Write RED tests for a natural non-backfill generation, restart after every
  cursor, mismatch receipts, exact legacy identity drift rejection, and empty /
  boundary universes. Assert every call reads <= `max_rows` comparison rows.
- [ ] Write RED SQL-trace/time tripwires proving the final pointer switch performs
  metadata queries only and does not call the one-shot hash helper or count/scan
  generation/legacy Structure tables.
- [ ] Initialize comparison progress only after generation certification freezes
  its validation hash; pin exact legacy snapshot metadata and canonical framing
  state; CAS each cursor/state transition; seal receipt and `ready` in one
  transaction after final framing bytes.
- [ ] Route backfill through the same runner and remove its one-shot receipt scan.
  Require a valid receipt digest before publish and bind it into the pointer.
- [ ] Run publication/readers suites until all restart, mismatch, and no-scan tests pass.

### Task 4: Regression, documentation, and atomic handoff

**Files:**
- Modify: `.superpowers/sdd/task-5-report.md`
- Modify only if hook requires: project manual sync log.

- [ ] Run all Task 3–5 certification, backfill, pointer, schema, migration, hot SQL
  trace, and consumer suites; record exact test/failure/skip totals.
- [ ] Run changed-file Ruff, `git diff --check`, `make planning-status`, and the
  repository pre-commit hook through one scoped commit.
- [ ] Append RED/GREEN evidence, canonical-hash proof, boundedness proof, migration
  semantics, and remaining non-rollout concerns to the Task 5 report.
- [ ] Stage only Task 5 refinement files and commit once as
  `fix(m1): seal bounded comparison receipts`.

### Task 5: Generic retention boundary

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_sqlite_store.py`

- [ ] Write a RED retention test with an otherwise-expired current generation,
  publication, sealed receipt, comparison progress, exact legacy identity, and
  generation rows plus one unrelated expired snapshot.
- [ ] Exclude every referenced evidence identity in the bounded candidate query;
  run keep-set selection, full evidence exclusion, and deletion under one
  `BEGIN IMMEDIATE`; do not delete or update sealed evidence and do not use FK
  rollback as filtering.
- [ ] Prove unrelated deletion succeeds, the full chain remains, and replay is
  idempotent. Inject a competing evidence writer after candidate-query execution
  and prove the writer lock closes the TOCTOU window. Document that generation
  reclamation needs a future dedicated bounded evidence-aware cleanup API before
  production closure.

## Self-review

- Spec coverage: canonical framing/state, four bounded phases, CAS/restarts,
  legacy drift, receipt digest/immutability, pointer repair, no-scan publication,
  backfill convergence, corruption checks, and final gates are each assigned.
- Placeholder scan: no TBD/TODO/FIXME or deferred behavior.
- Type consistency: the same serialized SHA state, receipt digest, progress key,
  and pointer binding are consumed by schema, writer, migration, and reader tasks.
