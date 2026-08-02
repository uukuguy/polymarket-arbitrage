# M1 Structure Drift-Safe Switch Gate Implementation Plan

> **Required workflow:** Execute task-by-task with test-driven development. Add
> the failing contract first, run it to observe the intended failure, implement
> the minimum production behavior, then run focused and regression gates before
> committing each task.

**Goal:** Authorize the production generation reader for generation 848 without
accepting stale legacy 845 as an exact temporal peer, while proving zero
unexpected overlap mutation and classifying the complete symmetric difference
from frozen source evidence.

**Architecture:** A scheduler-owned bounded state machine independently projects
the published generation source window through the legacy normalizers plus exact
quarantine policy. It byte-compares that projection with generation truth, then
partitions legacy/generation members into authenticated shared/addition/removal
classes. An append-only receipt binds the entire identity and is consumed by a
read-only preflight gate; the existing exact comparator stays unchanged.

**Design source:**
`docs/superpowers/specs/2026-08-02-m1-structure-drift-safe-switch-gate-design.md`

**Production constraint:** No task in this plan deploys, changes
`current_structure_generation`, changes configured read mode, or writes legacy
serving rows. Production activation and the eventual read-mode switch are
separate operator steps after verification.

---

## Task 1: Durable identity, append-only receipt, and evidence retention

**Files:**

- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_schema_lockstep.py`
- Test: `tests/m1-perception/test_structure_generation_readers.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`

### RED: define schema and migration contracts

Add failing tests that require:

- `structure_generation_drift_progress` with one active full comparison
  identity, fixed phase vocabulary, cursor, serializable digest state,
  per-class counts/digests, and checkpoint time;
- `structure_generation_drift_receipts` keyed by the full legacy/generation/
  publication/window/contract identity rather than generation ID alone;
- receipt fields for published source identity, exact receipt digest, pointer
  validation, source/projection/generation hashes, class commitments,
  reconstruction roots, conflict/unclassified commitments, and final digest;
- append-only update/delete triggers;
- idempotent schema initialization on a production-shaped 845/848 database;
- initialization creates no progress until explicitly admitted, does not alter
  the exact receipt or pointer, and does not rewrite legacy/generation rows;
- published source windows referenced by active drift progress or sealed drift
  receipts are excluded from staging retention; the exclusion selection and
  delete remain in one writer transaction;
- a receipt for one legacy identity cannot shadow or authorize another.

Run and observe failure:

```bash
uv run pytest -q tests/m1-perception/test_schema_lockstep.py \
  tests/m1-perception/test_structure_generation_readers.py \
  -k 'drift_schema or drift_receipt or drift_retention'
```

### GREEN: add schema and canonical digest primitives

Implement the DDL, lockstep migration, append-only triggers, typed progress and
receipt models, canonical receipt digest, and retention exclusions. Keep JSON
payloads canonical and bounded; reject unknown keys, negative counts,
non-64-character digests, conflict/unclassified success claims, and malformed
phase/cursor state.

Do not add any automatic progress initialization in schema bootstrap.

### Verify and commit

```bash
uv run pytest -q tests/m1-perception/test_schema_lockstep.py \
  tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_structure_generation_publication.py \
  -k 'drift or retention or comparison_receipt'
uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_schema_lockstep.py \
  tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_structure_generation_publication.py
git diff --check
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_schema_lockstep.py \
  tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_structure_generation_publication.py
git commit -m "feat(m1): persist structure drift comparison evidence"
```

---

## Task 2: Independent same-window compatibility projector

**Files:**

- Create: `src/polyarb/perception/structure_drift.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_structure_drift_projection.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`

### RED: prove independent projection and exact semantics

Add failing tests for a published window whose
`published_snapshot_id` matches the current generation. Prove that a bounded
projector:

- reads only frozen staging plus publication/pointer evidence and makes no
  network calls;
- invokes `normalize_events` and `normalize_market` on pinned raw payloads;
- does not read generation rows to derive expected rows;
- applies exact event-only and market-side quarantine receipts;
- recomputes membership hashes and fresh group truth after event-only removal;
- detects cross-event identity conflict with the publication contract;
- emits canonical legacy-reader eligible tuples in stable key order;
- rejects `complete`, failed, wrong-snapshot, mutable, or source-hash-drifted
  window identities;
- produces byte-identical expected universe/group-truth hashes for a matching
  independently built generation and fails on one changed structural byte;
- reads no more than 500 source rows and uses a constant number of SELECTs for a
  500-row chunk.

Run and observe failure:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py
```

### GREEN: implement pure projection and bounded source phases

Create canonical tuple helpers in `structure_drift.py`. Keep them pure: raw
payload and relationship/quarantine facts in, expected tuples out. Add bulk
store readers for event and market chunks, source identity revalidation, and
CAS checkpoint advancement for `source-events` and `source-markets`.

Persist only hash/count progress, not a second serving snapshot. Expected values
must never be copied from generation tables. Query generation only after the
expected chunk is independently constructed, for exact comparison.

### Verify and commit

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py \
  tests/m1-perception/test_structure_generation_publication.py \
  -k 'drift or source_events or source_markets'
uv run ruff check src/polyarb/perception/structure_drift.py \
  src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_projection.py
git diff --check
git add src/polyarb/perception/structure_drift.py \
  src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_projection.py \
  tests/m1-perception/test_structure_generation_publication.py
git commit -m "feat(m1): project same-window structure compatibility"
```

---

## Task 3: Exact overlap and full symmetric-difference classifier

**Files:**

- Modify: `src/polyarb/perception/structure_drift.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_structure_drift_classification.py`
- Test: `tests/m1-perception/test_structure_generation_readers.py`

### RED: define every allowed and forbidden partition

Build small deterministic legacy/generation/source fixtures and failing tests
for:

- shared member equality across event, group, market, member kind, active,
  closed, condition, YES/NO token, neg-risk, and incomplete fields;
- every individual shared field mutation becoming `overlap-conflict`;
- generation-only `fresh-addition` requiring complete pinned source,
  independent projector equality, generation certification, and no quarantine;
- mutually exclusive legacy-only reasons in fixed priority order:
  `current-nontradable`, `event-only-quarantine`, `market-side-quarantine`, and
  `fresh-source-absent`;
- exact event-only and market-side quarantine receipt recomputation;
- active source-present generation omission, ambiguous parentage, conflicting
  quarantine, duplicate membership, and unexplained one-catalogue presence
  becoming `unclassified`;
- full symmetric-difference accounting, including fixtures where additions and
  removals are both non-zero but the net count is zero or 6,048;
- tagged digest/reconstruction roots changing on row substitution, class
  substitution, duplication, omission, or order drift;
- member-level comparison accepting legitimate group hash change only when all
  underlying additions/removals are classified and fresh group truth recomputes;
- <=500 input rows and constant-count bulk SELECTs per chunk.

Run and observe failure:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_classification.py
```

### GREEN: implement bounded generation and legacy phases

Implement `generation-members`, `legacy-members`, and `fresh-group-truth` phases
as keyset streams. Use bounded bulk joins/lookups and canonical tagged records.
Count shared rows once, generation-only rows on the generation pass, and only
generation-absent legacy rows on the legacy pass. Enforce reason exclusivity;
never select the first convenient reason without proving the other predicates
do not also match.

At every chunk boundary revalidate source, legacy, generation, publication,
pointer, exact receipt, contract, and certification identity before CAS commit.

### Verify and commit

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_classification.py \
  tests/m1-perception/test_structure_generation_readers.py -k 'drift'
uv run ruff check src/polyarb/perception/structure_drift.py \
  src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_classification.py
git diff --check
git add src/polyarb/perception/structure_drift.py \
  src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_classification.py \
  tests/m1-perception/test_structure_generation_readers.py
git commit -m "feat(m1): classify authenticated structure drift"
```

---

## Task 4: Seal and consume the drift-safe authorization

**Files:**

- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `src/polyarb/snapshot/cli.py`
- Modify: `Makefile`
- Test: `tests/m1-perception/test_structure_generation_readers.py`
- Test: `tests/m1-perception/test_makefile_contract.py`
- Test: `tests/m1-perception/test_snapshot_cli_json.py`

### RED: define fail-closed seal and read-only gate

Add failing tests proving sealing succeeds only when:

- source projection and generation universe/group truth match exactly;
- shared plus one-sided partitions reconstruct both universes;
- every legacy-only row belongs to exactly one reason;
- conflict and unclassified counts are zero;
- the current legacy, pointer, publication, published window, contract, exact
  receipt, validation, certification, counts, and hashes still match progress.

Prove receipt authentication rejects every field substitution, update/delete,
and replay after legacy or pointer drift. Prove the existing
`structure-generation-compare` output and exit code remain unchanged.

Add the required Makefile entry:

- `make structure-generation-drift-compare` — read-only current progress,
  receipt, class counts, hashes, identities, and authorization result; never
  advances a chunk or changes read mode.

The new CLI exits zero only for exact authorization or a sealed drift-safe
receipt on the current full identity. Incomplete, stale, conflicting, malformed,
or unauthenticated results exit non-zero with stable JSON.

Run and observe failure:

```bash
uv run pytest -q tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_makefile_contract.py \
  tests/m1-perception/test_snapshot_cli_json.py -k 'drift or structure_generation_compare'
```

### GREEN: seal receipt and expose read-only preflight

Implement one sealing transaction, canonical receipt verification, typed result,
read-only comparison API, CLI, and Makefile target. The hot generation reader
must consume only the sealed identity/digest and must not scan comparison or
source rows. Do not change `STRUCTURE_GENERATION_READ_MODE` here.

### Verify and commit

```bash
uv run pytest -q tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_makefile_contract.py \
  tests/m1-perception/test_snapshot_cli_json.py \
  -k 'drift or structure_generation_compare or generation_read'
uv run ruff check src/polyarb/storage/sqlite_store.py src/polyarb/snapshot/cli.py \
  tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_makefile_contract.py \
  tests/m1-perception/test_snapshot_cli_json.py
git diff --check
git add src/polyarb/storage/sqlite_store.py src/polyarb/snapshot/cli.py Makefile \
  tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_makefile_contract.py \
  tests/m1-perception/test_snapshot_cli_json.py
git commit -m "feat(m1): seal drift-safe structure authorization"
```

---

## Task 5: Scheduler ownership, default-off config, health, and production 845/848 resume

**Files:**

- Modify: `src/polyarb/config.py`
- Modify: `src/polyarb/daemon/scheduler.py`
- Modify: `src/polyarb/http/health.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_scheduler.py`
- Test: `tests/m1-perception/test_health_endpoint.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`
- Test: `tests/m1-perception/test_snapshot_cli_json.py`

### RED: define sole production advancement authority

Add failing tests that require:

- `structure_generation_drift_compare_enabled=False` by default;
- disabled scheduler never creates or advances progress;
- enabled scheduler selects a pending current-pointer drift comparison before a
  new Structure collection window;
- it owns `_tick_lock`, acquires `producer_lock`, then performs both existing
  Quote-priority checks before initialization or chunk advancement;
- active/due Quote records the existing defer receipt and leaves progress
  unchanged;
- writer busy or CAS loss defers without retry loops or failure-counter growth;
- one admitted tick advances only the bounded row/deadline budget and releases
  the producer slot on success, failure, timeout, or cancellation;
- an operator `request_now` follows the identical scheduler path;
- health reports enabled/disabled, phase, checkpoint age, full identity,
  projection match, class counts, conflict/unclassified counts, receipt state,
  authorization, stale reason, and starvation/defer evidence;
- health never calls a full source/universe scan;
- a production-shaped database with legacy 845, pointer/publication/generation
  848, authenticated failing exact receipt, and published window 97b initializes
  one resumable drift identity without modifying any of those rows;
- repeated scheduler ticks resume that exact identity after restart;
- source/pointer/legacy drift stales progress and starts no mixed replacement in
  the same tick;
- the 97b source window remains retention-protected through seal.

Run and observe failure:

```bash
uv run pytest -q tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_structure_generation_publication.py \
  -k 'drift'
```

### GREEN: integrate scheduler and observability

Add the default-off setting and a scheduler maintenance result type/stage. Reuse
the existing admission, producer lock, timeout, cancellation, attempt truth, and
defer mechanisms. Do not add a second lock or a direct production CLI writer.

Expose bounded stored drift status through Structure health. Keep failure facts
actionable but do not downgrade an otherwise healthy legacy data plane merely
because drift comparison is disabled or incomplete. A sealed mismatch or stale
enabled gate is visible and alertable.

### Verify and commit

```bash
uv run pytest -q tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_structure_generation_publication.py \
  tests/m1-perception/test_snapshot_cli_json.py -k 'drift or quote_priority'
uv run ruff check src/polyarb/config.py src/polyarb/daemon/scheduler.py \
  src/polyarb/http/health.py src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_structure_generation_publication.py
git diff --check
git add src/polyarb/config.py src/polyarb/daemon/scheduler.py \
  src/polyarb/http/health.py src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_structure_generation_publication.py \
  tests/m1-perception/test_snapshot_cli_json.py
git commit -m "feat(m1): schedule drift-safe structure comparison"
```

---

## Task 6: Learning/runbook documentation and full verification

**Files:**

- Create: `docs/learning/46-Structure时间漂移门禁.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `docs/dev/production-operations.md` if present; otherwise update the
  existing M1 production runbook selected during implementation
- Modify: `.planning/threads/market-observation-architecture.md`

### Document the production path

Write the 30-second mental model, code anchors, design trade-offs, adversarial
self-check questions, and FAQ increment. Document the operator sequence without
executing it:

1. deploy schema/code with drift comparison disabled;
2. verify pointer 848, exact receipt identity, published window 97b, source
   counts/hashes, and zero failure counter;
3. enable only `structure_generation_drift_compare_enabled`;
4. request or wait for scheduler-owned bounded ticks;
5. watch health until sealed, ensuring Quote defer/latency remains healthy;
6. run `make structure-generation-drift-compare` read-only preflight;
7. separately approve and change generation read mode;
8. retain legacy rollback identity and monitor first production reads.

Explicitly state that this plan stops before steps 1–8 mutate production.

### Full verification

```bash
make planning-status
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py \
  tests/m1-perception/test_structure_drift_classification.py \
  tests/m1-perception/test_structure_generation_publication.py \
  tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_schema_lockstep.py \
  tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_makefile_contract.py \
  tests/m1-perception/test_snapshot_cli_json.py
uv run ruff check src tests/m1-perception
uv run pytest -q
git diff --check
```

Record exact collected/pass/skip/xfail counts and performance evidence. Confirm
`git diff` contains no deployment, pointer, `.env`, secret, or production DB
change.

### Commit documentation

```bash
git add docs/learning/46-Structure时间漂移门禁.md docs/learning/00-INDEX.md \
  .planning/threads/market-observation-architecture.md
git add docs/dev/production-operations.md  # only if this is the selected runbook
git commit -m "docs(m1): explain drift-safe structure rollout"
```

Before claiming completion, run `make planning-status`, create any required plan
SUMMARY artifact, and perform verification-before-completion review. Do not
deploy or switch production read mode in this implementation session.
