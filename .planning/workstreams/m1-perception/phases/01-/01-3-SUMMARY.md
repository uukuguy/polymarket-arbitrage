---
phase: 01
plan: 3
wave: 2
type: execute
status: complete
started_at: 2026-04-29T07:48:38Z
completed_at: 2026-04-29T08:11:08Z
duration_min: 22
tasks_completed: 8
tests_passed: 35
note: "Resumed after socket-crash mid-Wave-2 (T1-T4 committed by previous executor; T5-T8 by this executor)."
provides:
  - storage.schemas.DDL
  - storage.schemas.SNAPSHOT_SCHEMA
  - storage.schemas.MARKETS_INSERT_SQL
  - storage.schemas.MARKETS_COLUMN_ORDER
  - storage.SQLiteStore
  - storage.parquet_writer.compute_snapshot_path
  - storage.parquet_writer.write_parquet_atomic
  - validator.Category
  - validator.Issue
  - validator.layer1_count
  - validator.layer2_fields
  - validator.layer4_cross_source
  - validator.is_valid_overall
key_files_created:
  - src/polyarb/storage/schemas.py
  - src/polyarb/storage/sqlite_store.py
  - src/polyarb/storage/parquet_writer.py
  - src/polyarb/validator/category.py
  - src/polyarb/validator/layers.py
  - tests/m1-perception/test_sqlite_store.py
  - tests/m1-perception/test_parquet_writer.py
  - tests/m1-perception/test_validator.py
---

# Phase 01 Plan 3: Storage + Validator — Summary

> **Resumed after socket-crash mid-Wave-2.** Tasks T1-T4 were committed by a previous
> executor session that crashed before Wave 2 finished. This session resumed from T5
> and completed T5-T8 + this SUMMARY. All 8 tasks now committed and verified.

Atomic SQLite + Parquet snapshot persistence layer with categorized 3-layer validator
(Layer 1 count strict, Layer 2 field presence with categorization heuristic, Layer 4
cross-source ghost-book defense for issue #180). Stdlib `sqlite3` with WAL +
BEGIN IMMEDIATE; explicit `pyarrow.Schema` (token IDs as `pa.string()` to survive
uint256); F-1 `_safe_float` and F-5 size caps applied throughout the validator.

## Per-task commits

| # | Task | Commit | Files | Tests |
|---|------|--------|-------|-------|
| T1 | SQLite DDL + pyarrow schema | `302c9e3` | `src/polyarb/storage/schemas.py` | — |
| T2 | SQLiteStore (stdlib + WAL) | `b18afad` | `src/polyarb/storage/sqlite_store.py` | — |
| T3 | Atomic Parquet writer | `f92185a` | `src/polyarb/storage/parquet_writer.py` | — |
| T4 | Category enum + Issue | `48a9f55` | `src/polyarb/validator/category.py` | — |
| T5 | Validator Layer 1/2/4 | `809bd3f` | `src/polyarb/validator/layers.py` | — |
| T6 | SQLiteStore tests | `1da888b` | `tests/m1-perception/test_sqlite_store.py` | 10 |
| T7 | Parquet writer tests | `3763770` | `tests/m1-perception/test_parquet_writer.py` | 7 |
| T8 | Validator tests | `4ba7274` | `tests/m1-perception/test_validator.py` | 18 |

`pytest tests/m1-perception/test_sqlite_store.py tests/m1-perception/test_parquet_writer.py tests/m1-perception/test_validator.py` → **35 passed in 0.47s**.

## Schema decisions (lock-step contract for Plan 4 / 5)

These three artifacts MUST stay aligned. Adding a column requires updating ALL
of: DDL, MARKETS_COLUMN_ORDER, MARKETS_INSERT_SQL, and SNAPSHOT_SCHEMA.

### `markets` table — column order (21 cols)

```python
MARKETS_COLUMN_ORDER = (
    "market_id", "condition_id", "slug", "question",
    "yes_token_id", "no_token_id",
    "mid_price", "liquidity_usd", "volume_usd",
    "best_bid_price", "best_bid_size", "best_ask_price", "best_ask_size",
    "end_time_ms",
    "active", "closed", "neg_risk", "neg_risk_market_id",
    "fetched_at_ms", "snapshot_id", "incomplete",
)
```

- SQLite types: `TEXT` for `*_id` / `slug` / `question`; `REAL` for prices/sizes/liquidity/volume; `INTEGER` for `end_time_ms` / `fetched_at_ms` / `snapshot_id` / boolean cols (`active`, `closed`, `neg_risk`, `incomplete`).
- `market_id` is PRIMARY KEY; `snapshot_id` is FK → `snapshots(id)`.
- `incomplete INTEGER NOT NULL DEFAULT 0` — Layer 2 mark-don't-drop signal.
- Indexes: `idx_markets_liquidity ON markets(liquidity_usd)`, `idx_markets_end_time ON markets(end_time_ms)`.

### `snapshots` table

```sql
id              INTEGER PRIMARY KEY AUTOINCREMENT
taken_at_ms     INTEGER NOT NULL
finished_at_ms  INTEGER NOT NULL
mode            TEXT NOT NULL CHECK(mode IN ('subset','full'))
market_count    INTEGER NOT NULL
is_valid        INTEGER NOT NULL    -- 0 or 1; per D-D3 still written when 0
parquet_path    TEXT NOT NULL
notes           TEXT
```

### `validation_issues` table

```sql
id           INTEGER PRIMARY KEY AUTOINCREMENT
snapshot_id  INTEGER NOT NULL REFERENCES snapshots(id)
layer        INTEGER NOT NULL          -- 1, 2, or 4 (Layer 3 deferred)
category     TEXT NOT NULL             -- enum value, e.g. 'ghost_book'
market_id    TEXT                      -- nullable: Layer 1 issues are not market-scoped
detail       TEXT                      -- ≤ 200 chars (F-5)
raw_payload  TEXT                      -- ≤ 1024 bytes (F-5); 500 for ghost-book payloads
```

Indexes: `idx_issues_snapshot`, `idx_issues_category`.

### `pyarrow.Schema` — `SNAPSHOT_SCHEMA` (Parquet)

22 fields = 21 markets columns minus `snapshot_id` (placeholder in DB) plus 2
parquet-only fields and a re-included `snapshot_id`:

| field | parquet type | notes |
|-------|-------------|-------|
| market_id | string | required |
| condition_id | string | required |
| slug | string (nullable) | |
| question | string (nullable) | |
| yes_token_id | **string (nullable)** | **Pitfall 3 — uint256 > int64** |
| no_token_id | **string (nullable)** | **Pitfall 3 — uint256 > int64** |
| mid_price | float64 (nullable) | |
| liquidity_usd | float64 (nullable) | |
| volume_usd | float64 (nullable) | |
| best_bid_price | float64 (nullable) | |
| best_bid_size | float64 (nullable) | |
| best_ask_price | float64 (nullable) | |
| best_ask_size | float64 (nullable) | |
| end_time_ms | int64 (nullable) | |
| active | bool | parquet uses bool, SQLite uses INTEGER 0/1 |
| closed | bool | |
| neg_risk | bool | |
| neg_risk_market_id | string (nullable) | |
| fetched_at_ms | int64 | per-row stamp documents Pitfall 6 best-effort consistency |
| snapshot_taken_at_ms | int64 | parquet-only — for archive partitioning by date |
| snapshot_id | int64 | parquet-only — FK reference to SQLite snapshots.id |
| incomplete | bool | Layer 2 mark-don't-drop signal |

Parquet path layout (`compute_snapshot_path`):
```
{parquet_root}/YYYY/MM/DD/HH-MM-SS.parquet   (UTC)
```

### `Issue` dataclass — wire signature for orchestrator (Plan 4)

```python
@dataclass(frozen=True)
class Issue:
    layer: int                  # 1, 2, or 4
    category: Category          # enum, NOT bare string (writer extracts .value)
    market_id: str | None       # None for Layer 1 (count not market-scoped)
    detail: str                 # caller is responsible for ≤ 200 chars (validator does this)
    raw_payload: str | None = None
```

Plan 4 orchestrator must construct Issue instances via the dataclass — passing
strings or dicts will fail. `Category` has the `(str, Enum)` mixin so values
serialize to SQLite TEXT directly via `issue.category.value`.

### `Category` enum (7 members)

```
zombie_market  resolving  api_jitter  api_unreachable  clob_missing  ghost_book  unknown
```

Per D-D4: `unknown` is allowed but should NOT persist in steady state — every
recurring `unknown` is a system debt to be classified into one of the others.

## Validity policy (resolved Q5)

`is_valid_overall(issues)` returns `False` **iff** any `Issue.layer == 1` is present.

- Layer 1 (count strict): mismatch → `is_valid=False` → orchestrator must use non-zero exit code (D-D3 still persists the row so the failure is queryable).
- Layer 2 (field presence): record-only — issues persisted, `is_valid` unchanged.
- Layer 4 (cross-source / ghost-book): record-only — issues persisted, `is_valid` unchanged.

This is intentionally permissive for Phase 1; it gives us evidence to set
thresholds in Phase 3 once we observe real-world Layer 2/4 issue rates.

## Layer 2 categorization heuristic — verified as written

No adjustment was needed during T8. The heuristic order is:

1. `end_time_ms` set AND `0 < (end_time_ms - now_ms) < 24h` → `RESOLVING`
2. else `liquidity_usd is not None AND liquidity_usd < $10` → `ZOMBIE_MARKET`
3. else → `UNKNOWN`

Tests:
- `test_layer2_missing_field_categorizes_resolving_when_endtime_near` (1)
- `test_layer2_missing_field_marks_incomplete_and_categorizes_zombie` (2)
- `test_layer2_missing_field_categorizes_unknown_when_no_heuristic_matches` (3)

A guarded `0 < delta` check prevents past-end_time markets from being misclassified as RESOLVING (RESOLVING means about-to-resolve, not already-resolved).

## Sample debug query (operator handbook)

After a snapshot run, count categorized failures from the latest snapshot:

```sql
SELECT category, COUNT(*) AS n
FROM validation_issues
WHERE snapshot_id = (SELECT MAX(id) FROM snapshots)
GROUP BY category
ORDER BY n DESC;
```

Or join to inspect the offending markets:

```sql
SELECT v.layer, v.category, m.market_id, m.liquidity_usd, m.end_time_ms, v.detail
FROM validation_issues v
LEFT JOIN markets m USING (market_id)
WHERE v.snapshot_id = (SELECT MAX(id) FROM snapshots)
ORDER BY v.layer, v.category;
```

To see how often is_valid=False (Layer 1 jitter) over time:

```sql
SELECT DATE(taken_at_ms / 1000, 'unixepoch') AS day,
       SUM(CASE WHEN is_valid=0 THEN 1 ELSE 0 END) AS jitters,
       COUNT(*) AS total
FROM snapshots
GROUP BY day
ORDER BY day DESC;
```

## Security invariants applied

- **F-1** (input safety) — `validator/layers.py` `_safe_float` catches `(KeyError, TypeError, ValueError)` on every coercion of CLOB-controlled fields. Unparseable books are surfaced as `Category.UNKNOWN` Layer 4 issue with `raw_payload` truncated to 500 bytes; ghost-book check is skipped for that token. The bonus test `test_layer4_unparseable_book_does_not_crash` confirms no `ValueError` propagates.
- **F-5** (size caps) — `detail` capped at 200 chars and `raw_payload` at 1024 bytes (500 bytes for ghost-book book payloads). Prevents a market with a 10MB `question` field from inflating `validation_issues` without bound.
- **Pitfall 3** (uint256 token IDs) — `pa.string()` for `yes_token_id` / `no_token_id` in SNAPSHOT_SCHEMA + `TEXT` in DDL. Verified by both `test_token_ids_preserve_uint256_string` (SQLite) and `test_write_parquet_token_id_preserved_as_string` (Parquet).
- **Anti-pattern #1** (atomic markets replace) — `BEGIN IMMEDIATE` + `DELETE FROM markets` + `executemany INSERT`, NOT `INSERT OR REPLACE`. Verified by `test_write_snapshot_overwrites_markets`. Also: a failed transaction does not delete prior markets, verified by `test_rollback_on_executemany_failure`.

## Deviations from plan

### From this session's run (T5-T8)

**[Rule 3 — tooling]** `pytest` invocations from the agent's bash shell get intercepted by a token-saving wrapper that emits the canned output `Pytest: No tests collected` regardless of actual collection. Bypass: ran tests via `rtk proxy python -m pytest …`. **Action for orchestrator/Plan 4**: prefer `python -m pytest` invocations over the `pytest` binary in agent contexts; do NOT take "no tests collected" at face value without `rtk proxy`. No code change required.

**[Bonus test]** Added an 18th validator test (`test_layer4_unparseable_book_does_not_crash`) on top of the 17 the plan listed, exercising F-1 directly. Plan goal "≥34 tests" is met (35 in total).

**[Filename normalization]** The orchestration prompt referred to `tests/m1-perception/test_storage.py`, but the plan frontmatter / body use `tests/m1-perception/test_sqlite_store.py`. Followed the plan (canonical source). No conflict — the file did not previously exist under either name.

### From the previous (crashed) session's run (T1-T4)

None observed — files match the plan spec exactly. The crash interrupted Wave 2 between T4 (committed `48a9f55` at 15:49) and the start of T5; no partial files or leftover state.

## Authentication gates

None — this is a pure-Python implementation plan with no external service interaction.

## Known stubs

None — every artifact is fully wired and exercised by tests.

## Threat flags

None — this plan is purely internal storage + validation logic; no new network endpoints, auth paths, file access patterns, or schema changes at trust boundaries beyond what was already covered in the plan's `<threat_model>`.

## Open items

1. **Plan 4 orchestrator** must:
   - Build market dicts containing `MARKETS_COLUMN_ORDER` keys (or a superset; missing keys are stored as NULL).
   - For Parquet: also populate `snapshot_taken_at_ms` and translate booleans to Python `bool` (vs `int` for SQLite).
   - Construct `Issue` dataclasses (not bare dicts) before passing to `SQLiteStore.write_snapshot`.
   - Set process exit code based on `is_valid_overall(all_issues)` (D-D3).
   - Compute snapshot path via `compute_snapshot_path(parquet_root, taken_at_ms)` and write Parquet BEFORE SQLite (so `parquet_path` is known when the snapshot row is inserted; rollback the .tmp file or written .parquet on SQLite failure).

2. **Plan 5 conftest** should set `POLYARB_ALLOW_EXTERNAL_PATHS=1` globally (each test file in this plan does it locally as a belt-and-suspenders). Once conftest exists, the per-file `os.environ.setdefault(...)` lines can stay (idempotent) or be cleaned up at integrator's discretion.

3. **Layer 3** is intentionally deferred to Phase 3 (sequential snapshot diff — needs a baseline window of historical snapshots, which we don't have until Phase 2 ships).

## Self-Check: PASSED

Verified post-write:

- `src/polyarb/validator/layers.py` — FOUND
- `tests/m1-perception/test_sqlite_store.py` — FOUND
- `tests/m1-perception/test_parquet_writer.py` — FOUND
- `tests/m1-perception/test_validator.py` — FOUND
- T1 commit `302c9e3` — FOUND in git log
- T2 commit `b18afad` — FOUND in git log
- T3 commit `f92185a` — FOUND in git log
- T4 commit `48a9f55` — FOUND in git log
- T5 commit `809bd3f` — FOUND in git log
- T6 commit `1da888b` — FOUND in git log
- T7 commit `3763770` — FOUND in git log
- T8 commit `4ba7274` — FOUND in git log
- 35 tests pass under `python -m pytest` (10 sqlite + 7 parquet + 18 validator)
- Combined import smoke `STORAGE_VALIDATOR_OK` — confirmed
