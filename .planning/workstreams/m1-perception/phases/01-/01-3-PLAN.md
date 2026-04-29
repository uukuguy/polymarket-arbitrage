---
phase: 01
plan: 3
type: execute
wave: 2
depends_on: [01-1]
files_modified:
  - src/polyarb/storage/schemas.py
  - src/polyarb/storage/sqlite_store.py
  - src/polyarb/storage/parquet_writer.py
  - src/polyarb/validator/category.py
  - src/polyarb/validator/layers.py
  - tests/m1-perception/test_sqlite_store.py
  - tests/m1-perception/test_parquet_writer.py
  - tests/m1-perception/test_validator.py
autonomous: true
requirements: []
must_haves:
  truths:
    - "schemas.py defines DDL string for 3 tables (snapshots, markets, validation_issues) + pyarrow.Schema with column names matching SQLite columns"
    - "SQLiteStore.replace_markets uses BEGIN IMMEDIATE + DELETE FROM markets + executemany INSERT (covers PATTERNS.md anti-pattern #1)"
    - "SQLite WAL journal mode enabled at init (so reads do not block writer)"
    - "snapshots table is append-only (one row per snapshot run); markets table is overwritten atomically"
    - "validation_issues table records (snapshot_id, layer, category, market_id, detail, raw_payload) per Issue"
    - "ParquetWriter writes via tmp + os.replace (atomic), uses explicit pa.Schema (NOT inferred), uses snappy compression"
    - "Parquet path follows YYYY/MM/DD/HH-MM-SS.parquet pattern under settings.parquet_root"
    - "Validator Layer 1 strict equality flips is_valid=false; Layer 2/4 issues recorded but is_valid stays true"
    - "Layer 4 ghost-book detection (issue #180): if best_ask>0.98 AND best_bid<0.02 AND |get_price - best_ask|>0.05 → Category.GHOST_BOOK"
    - "Issue dataclass + Category(str, Enum) live in validator/category.py (per resolved Q6)"
    - "Per-row fetched_at_ms column accepted by both SQLite and Parquet schemas (atomicity disclaimer)"
  artifacts:
    - path: src/polyarb/storage/schemas.py
      provides: "SQLite DDL string + pyarrow.Schema (column-aligned)"
      exports: ["DDL", "SNAPSHOT_SCHEMA"]
    - path: src/polyarb/storage/sqlite_store.py
      provides: "BEGIN IMMEDIATE writer (replace_markets, record_snapshot, record_issues)"
      exports: ["SQLiteStore"]
    - path: src/polyarb/storage/parquet_writer.py
      provides: "Atomic Parquet writer (tmp + os.replace)"
      exports: ["write_parquet_atomic", "compute_snapshot_path"]
    - path: src/polyarb/validator/category.py
      provides: "Category enum + Issue dataclass"
      exports: ["Category", "Issue"]
    - path: src/polyarb/validator/layers.py
      provides: "layer1_count, layer2_fields, layer4_cross_source, is_valid_overall"
      exports: ["layer1_count", "layer2_fields", "layer4_cross_source", "is_valid_overall"]
  key_links:
    - from: "src/polyarb/storage/sqlite_store.py"
      to: "src/polyarb/storage/schemas.py:DDL"
      via: "init_schema() executescript(DDL)"
      pattern: "executescript.*DDL"
    - from: "src/polyarb/storage/parquet_writer.py"
      to: "src/polyarb/storage/schemas.py:SNAPSHOT_SCHEMA"
      via: "pa.Table.from_pylist(rows, schema=SNAPSHOT_SCHEMA)"
      pattern: "from_pylist.*SNAPSHOT_SCHEMA"
    - from: "src/polyarb/validator/layers.py:layer4_cross_source"
      to: "books_by_token + prices_by_token (from Plan 2 ClobReaderClient)"
      via: "compares book[asks][0][price] vs prices_by_token[tid]['buy'] for ghost-book detection"
      pattern: "GHOST_BOOK"
---

<objective>
Build the data layer + validator. Three storage artifacts (`schemas.py`, `sqlite_store.py`, `parquet_writer.py`) handle persistence; two validator artifacts (`category.py`, `layers.py`) handle the 3 enabled validation layers (Layer 1 count strict, Layer 2 field presence, Layer 4 cross-source incl. ghost-book defense for issue #180).

Critical invariants:
- SQLite uses stdlib sqlite3 + DDL string (NOT SQLAlchemy ORM, per PATTERNS.md anti-pattern note)
- BEGIN IMMEDIATE + DELETE FROM markets + executemany — never INSERT OR REPLACE alone (anti-pattern #1)
- Parquet uses explicit pa.Schema with token_id as pa.string() (Pitfall 3 — uint256 overflows int64)
- Atomic write via tmp + os.replace (Pattern 4)
- Validator Layer 1 mismatch flips is_valid=false; Layer 2/4 issues recorded but don't flip is_valid (resolved Q5: Phase 1 = no threshold)
- Per-row `fetched_at_ms` column documents the best-effort consistency disclaimer (Pitfall 6)

Output: 5 source files + 3 test files.
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md
@.planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md
@.planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md
@.planning/workstreams/m1-perception/phases/01-/01-1-SUMMARY.md
@src/polyarb/config.py
</context>

<interfaces>
Settings (from Plan 1):
- db_path: Path  (default: data/state.db)
- parquet_root: Path  (default: data/snapshots)

Downstream consumers (Plan 4 orchestrator) will:
1. Build a list of normalized market dicts (rows)
2. Run validator layers → list[Issue]
3. Compute is_valid via is_valid_overall(issues)
4. Compute parquet path via compute_snapshot_path(parquet_root, taken_at_ms)
5. Write parquet via write_parquet_atomic(rows, path)
6. Write SQLite via SQLiteStore.write_snapshot(snapshot_meta, rows, issues)
</interfaces>

## Goal

A storage layer that atomically replaces the markets table inside a single SQLite transaction, records snapshot metadata + categorized validation issues, and emits a Parquet archive with stable explicit schema. A validator that runs three independent layers, returns a list of Issue records with mandatory `category`, and exposes a single `is_valid_overall` predicate following the Phase 1 rule (Layer 1 strict, Layer 2/4 record-only).

<tasks>

<task type="auto">
  <id>T1</id>
  <name>Task 1: Define schemas.py (SQLite DDL + pyarrow.Schema, column-aligned)</name>
  <files>src/polyarb/storage/schemas.py</files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Pattern 3 lines 366-419 for DDL; Pattern 4 lines 476-499 for pa.Schema; Pitfall 3 token_id must be string)
    - .planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md (D-C3 — three tables required)
    - .planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md (Plan 3 — schemas.py)
  </read_first>
  <action>
    Create `src/polyarb/storage/schemas.py` exporting two module-level constants:

    1. `DDL: str` — multi-line SQL string with these statements (in this order):
       - `PRAGMA journal_mode = WAL;`
       - `PRAGMA synchronous = NORMAL;`
       - `PRAGMA foreign_keys = ON;`
       - `CREATE TABLE IF NOT EXISTS snapshots (...)` with columns: `id INTEGER PRIMARY KEY AUTOINCREMENT`, `taken_at_ms INTEGER NOT NULL`, `finished_at_ms INTEGER NOT NULL`, `mode TEXT NOT NULL CHECK(mode IN ('subset','full'))`, `market_count INTEGER NOT NULL`, `is_valid INTEGER NOT NULL`, `parquet_path TEXT NOT NULL`, `notes TEXT`
       - `CREATE TABLE IF NOT EXISTS markets (...)` with columns (per RESEARCH.md Pattern 3 lines 382-405): `market_id TEXT PRIMARY KEY`, `condition_id TEXT NOT NULL`, `slug TEXT`, `question TEXT`, `yes_token_id TEXT`, `no_token_id TEXT`, `mid_price REAL`, `liquidity_usd REAL`, `volume_usd REAL`, `best_bid_price REAL`, `best_bid_size REAL`, `best_ask_price REAL`, `best_ask_size REAL`, `end_time_ms INTEGER`, `active INTEGER`, `closed INTEGER`, `neg_risk INTEGER`, `neg_risk_market_id TEXT`, `fetched_at_ms INTEGER NOT NULL`, `snapshot_id INTEGER NOT NULL REFERENCES snapshots(id)`, `incomplete INTEGER NOT NULL DEFAULT 0`
       - `CREATE INDEX IF NOT EXISTS idx_markets_liquidity ON markets(liquidity_usd);`
       - `CREATE INDEX IF NOT EXISTS idx_markets_end_time ON markets(end_time_ms);`
       - `CREATE TABLE IF NOT EXISTS validation_issues (...)` with columns: `id INTEGER PRIMARY KEY AUTOINCREMENT`, `snapshot_id INTEGER NOT NULL REFERENCES snapshots(id)`, `layer INTEGER NOT NULL`, `category TEXT NOT NULL`, `market_id TEXT`, `detail TEXT`, `raw_payload TEXT`
       - `CREATE INDEX IF NOT EXISTS idx_issues_snapshot ON validation_issues(snapshot_id);`
       - `CREATE INDEX IF NOT EXISTS idx_issues_category ON validation_issues(category);`

    2. `SNAPSHOT_SCHEMA: pa.Schema` — explicit pyarrow schema matching markets columns plus 2 extras for parquet (`snapshot_taken_at_ms: int64`, `snapshot_id: int64`). Per RESEARCH.md Pattern 4 lines 476-499. Critical: `yes_token_id` and `no_token_id` are `pa.string()` (Pitfall 3 — uint256 cannot fit int64). `incomplete` is `pa.bool_()` (Parquet) but stored as `INTEGER` in SQLite — the writer is responsible for the bool→int translation; the schemas declare each side's native type.

    3. Module-level constant `MARKETS_INSERT_SQL: str` — the parameterized INSERT statement matching the 21 markets columns (in same order as DDL declares them):
       ```sql
       INSERT INTO markets(market_id,condition_id,slug,question,yes_token_id,no_token_id,
       mid_price,liquidity_usd,volume_usd,best_bid_price,best_bid_size,best_ask_price,
       best_ask_size,end_time_ms,active,closed,neg_risk,neg_risk_market_id,
       fetched_at_ms,snapshot_id,incomplete) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
       ```

    4. Module-level constant `MARKETS_COLUMN_ORDER: tuple[str, ...]` — the 21 column names in the same order, for the writer to use when projecting dict→tuple.

    5. Add a comment at the top of the file:
       ```python
       # MARKETS_COLUMN_ORDER, MARKETS_INSERT_SQL, and SNAPSHOT_SCHEMA must stay in lockstep.
       # Adding a column requires updating ALL THREE plus the DDL.
       ```

    Use `import pyarrow as pa` at top. Do NOT use ORM, dataclass, or any other abstraction — just module-level string + Schema constants.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "
from polyarb.storage.schemas import DDL, SNAPSHOT_SCHEMA, MARKETS_INSERT_SQL, MARKETS_COLUMN_ORDER
import pyarrow as pa
assert 'CREATE TABLE IF NOT EXISTS snapshots' in DDL
assert 'CREATE TABLE IF NOT EXISTS markets' in DDL
assert 'CREATE TABLE IF NOT EXISTS validation_issues' in DDL
assert 'PRAGMA journal_mode = WAL' in DDL
assert 'idx_markets_liquidity' in DDL
assert 'idx_issues_category' in DDL
assert isinstance(SNAPSHOT_SCHEMA, pa.Schema)
assert SNAPSHOT_SCHEMA.field('yes_token_id').type == pa.string()
assert SNAPSHOT_SCHEMA.field('no_token_id').type == pa.string()
assert len(MARKETS_COLUMN_ORDER) == 21
assert MARKETS_INSERT_SQL.count('?') == 21
print('SCHEMAS_OK')
"</automated>
  </verify>
  <done>schemas.py exports DDL (3 tables + indexes + PRAGMAs), SNAPSHOT_SCHEMA (pa.Schema with token_id as string), MARKETS_INSERT_SQL (21 placeholders), MARKETS_COLUMN_ORDER (21 names); all imports succeed</done>
</task>

<task type="auto">
  <id>T2</id>
  <name>Task 2: Implement SQLiteStore (BEGIN IMMEDIATE + atomic markets replace)</name>
  <files>src/polyarb/storage/sqlite_store.py</files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Pattern 3 lines 360-462 — the writer pattern; Pitfall 4 — WAL + isolation_level=None)
    - .planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md (Plan 3 — sqlite_store; "What NOT to copy from reference" — no SQLAlchemy)
    - src/polyarb/storage/schemas.py (T1)
    - .planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md (D-C1 overwrite, D-C3 three tables, D-D3 is_valid still writes, D-D4 category required)
  </read_first>
  <action>
    Create `src/polyarb/storage/sqlite_store.py` exporting one class `SQLiteStore`:

    Required structure:
    - `from __future__ import annotations`
    - Imports: `sqlite3`, `from pathlib import Path`, `from loguru import logger`, `from polyarb.storage.schemas import DDL, MARKETS_INSERT_SQL, MARKETS_COLUMN_ORDER`, `from polyarb.validator.category import Issue` (forward import — Plan 3 owns both modules so import order works)

    - Class `SQLiteStore`:
      - `__init__(self, db_path: Path)`:
        - Store `self._db_path = Path(db_path)`; ensure parent dir exists (`self._db_path.parent.mkdir(parents=True, exist_ok=True)`)
      - `def init_schema(self) -> None`:
        - Open connection `con = sqlite3.connect(self._db_path, isolation_level=None)`; `con.executescript(DDL)`; `con.close()`
        - This is idempotent — safe to call before every snapshot
      - `def write_snapshot(self, *, taken_at_ms: int, finished_at_ms: int, mode: str, parquet_path: str, is_valid: bool, market_rows: list[dict], issues: list[Issue], notes: str | None = None) -> int`:
        - This is the SOLE write entry point — one transaction wraps everything
        - Validate `mode in ("subset", "full")` — raise ValueError if not
        - Open `con = sqlite3.connect(self._db_path, isolation_level=None)`; set PRAGMAs (`con.execute("PRAGMA journal_mode=WAL")`, `con.execute("PRAGMA synchronous=NORMAL")`)
        - `con.execute("BEGIN IMMEDIATE")`
        - Try block:
          - `con.execute("DELETE FROM markets")`  # full overwrite per D-C1 anti-pattern note
          - Insert snapshot meta row: `cur = con.execute("INSERT INTO snapshots(taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path,notes) VALUES (?,?,?,?,?,?,?)", (taken_at_ms, finished_at_ms, mode, len(market_rows), int(is_valid), parquet_path, notes))`; `snapshot_id = cur.lastrowid`
          - Build market tuples: for each row, project to `MARKETS_COLUMN_ORDER` order, convert `bool`→`int` for `active`/`closed`/`neg_risk`/`incomplete`, ensure `snapshot_id` is set (override row's value with the new id). Use a helper `_row_to_tuple(row, snapshot_id)`.
          - `con.executemany(MARKETS_INSERT_SQL, [_row_to_tuple(r, snapshot_id) for r in market_rows])`
          - Build issue tuples: for each Issue, `(snapshot_id, issue.layer, issue.category.value, issue.market_id, issue.detail, issue.raw_payload)`
          - `con.executemany("INSERT INTO validation_issues(snapshot_id,layer,category,market_id,detail,raw_payload) VALUES (?,?,?,?,?,?)", issue_tuples)`
          - `con.execute("COMMIT")`
        - Except: `con.execute("ROLLBACK")`; `raise` (don't swallow)
        - Finally: `con.close()`
        - Return `snapshot_id`

    - Add docstring at top documenting: "Per D-D3, write_snapshot persists the row even when is_valid=False so validation failures become queryable. Caller (orchestrator) is responsible for setting non-zero exit code based on is_valid."

    Type hints: Python 3.12 syntax. No try/except swallowing.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "
from polyarb.storage.sqlite_store import SQLiteStore
import inspect
sig = inspect.signature(SQLiteStore.write_snapshot)
expected = {'taken_at_ms','finished_at_ms','mode','parquet_path','is_valid','market_rows','issues','notes'}
assert expected <= set(sig.parameters), f'missing params: {expected - set(sig.parameters)}'
import tempfile, pathlib
with tempfile.TemporaryDirectory() as td:
    s = SQLiteStore(pathlib.Path(td)/'test.db')
    s.init_schema()
    import sqlite3
    con = sqlite3.connect(pathlib.Path(td)/'test.db')
    tables = [r[0] for r in con.execute(\"SELECT name FROM sqlite_master WHERE type='table'\")]
    assert 'snapshots' in tables and 'markets' in tables and 'validation_issues' in tables, tables
    print('STORE_OK')
"</automated>
  </verify>
  <done>SQLiteStore exposes init_schema + write_snapshot; init_schema creates 3 tables; write_snapshot signature accepts the 8 required keyword args; transaction discipline (BEGIN IMMEDIATE + COMMIT/ROLLBACK) implemented</done>
</task>

<task type="auto">
  <id>T3</id>
  <name>Task 3: Implement parquet_writer (atomic tmp + os.replace + explicit schema)</name>
  <files>src/polyarb/storage/parquet_writer.py</files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Pattern 4 lines 467-510; Pitfall 3 token_id; Pitfall 7 hatchling)
    - src/polyarb/storage/schemas.py (T1) — must use SNAPSHOT_SCHEMA
    - .planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md (D-C2 path pattern YYYY/MM/DD/HH-MM-SS.parquet)
  </read_first>
  <action>
    Create `src/polyarb/storage/parquet_writer.py` exporting two functions:

    1. `def compute_snapshot_path(parquet_root: Path, taken_at_ms: int) -> Path`:
       - Convert `taken_at_ms` to UTC datetime: `dt = datetime.fromtimestamp(taken_at_ms / 1000, tz=timezone.utc)`
       - Build path: `parquet_root / dt.strftime("%Y") / dt.strftime("%m") / dt.strftime("%d") / dt.strftime("%H-%M-%S.parquet")`
       - Return the Path (do NOT mkdir — that's the writer's job)

    2. `def write_parquet_atomic(rows: list[dict], out_path: Path) -> None`:
       - `out_path.parent.mkdir(parents=True, exist_ok=True)`
       - Build pyarrow table: `table = pa.Table.from_pylist(rows, schema=SNAPSHOT_SCHEMA)` — explicit schema is mandatory (Pitfall 3)
       - Atomic write:
         ```python
         tmp = out_path.with_suffix(out_path.suffix + ".tmp")
         pq.write_table(table, tmp, compression="snappy")
         os.replace(tmp, out_path)
         ```
       - On exception during `write_table`: ensure tmp file is cleaned up (`tmp.unlink(missing_ok=True)`), then re-raise
       - Log via loguru: `logger.info(f"Parquet written: {out_path} ({len(rows)} rows)")`

    Imports: `os`, `from pathlib import Path`, `from datetime import datetime, timezone`, `import pyarrow as pa`, `import pyarrow.parquet as pq`, `from loguru import logger`, `from polyarb.storage.schemas import SNAPSHOT_SCHEMA`.

    Document at top: "Atomic write: tmp file in same directory + os.replace (POSIX + Windows atomic per docs.python.org/3/library/os.html#os.replace). Failed writes leave only .tmp which the orchestrator removes on rollback."

    DO NOT add a class wrapper — these are pure functions.
    DO NOT add row schema coercion (e.g., float for prices) — caller (orchestrator) is responsible for shaping rows to match SNAPSHOT_SCHEMA. If rows mismatch, pyarrow raises and we let it propagate.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "
from polyarb.storage.parquet_writer import compute_snapshot_path, write_parquet_atomic
from pathlib import Path
import tempfile
p = compute_snapshot_path(Path('/tmp/snaps'), 1714435200000)  # 2024-04-30 00:00:00 UTC
assert p.parent.parent.parent.parent == Path('/tmp/snaps')
assert p.suffix == '.parquet'
print('PATH_OK', p)
" && grep -q "os.replace" src/polyarb/storage/parquet_writer.py && grep -q "SNAPSHOT_SCHEMA" src/polyarb/storage/parquet_writer.py && grep -q "compression=\"snappy\"" src/polyarb/storage/parquet_writer.py && echo IMPORTS_OK</automated>
  </verify>
  <done>parquet_writer.py exposes compute_snapshot_path + write_parquet_atomic; path follows YYYY/MM/DD/HH-MM-SS.parquet; explicit SNAPSHOT_SCHEMA used; atomic tmp+os.replace; snappy compression</done>
</task>

<task type="auto">
  <id>T4</id>
  <name>Task 4: Define validator/category.py (Category enum + Issue dataclass)</name>
  <files>src/polyarb/validator/category.py</files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Pattern 5 lines 525-540; Pitfall 1 — GHOST_BOOK)
    - .planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md (D-D4 — known categories list)
    - .planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md (Plan 3 — category.py; resolved Q6 — Issue lives here, not separate issues.py)
  </read_first>
  <action>
    Create `src/polyarb/validator/category.py` exporting two symbols:

    1. `class Category(str, Enum)` — the `(str, Enum)` mixin is mandatory (lets enum value serialize directly to SQLite TEXT). Members:
       - `ZOMBIE_MARKET = "zombie_market"`
       - `RESOLVING = "resolving"`
       - `API_JITTER = "api_jitter"`
       - `API_UNREACHABLE = "api_unreachable"`
       - `CLOB_MISSING = "clob_missing"`
       - `GHOST_BOOK = "ghost_book"`  # ⚠️ issue #180 defense
       - `UNKNOWN = "unknown"`  # never tolerate persistent unknowns — converge to specifics

    2. `@dataclass(frozen=True) class Issue`:
       - `layer: int`  # 1, 2, or 4
       - `category: Category`
       - `market_id: str | None`  # nullable: Layer 1 issues are not market-scoped
       - `detail: str`
       - `raw_payload: str | None = None`  # JSON string of the offending data, optional

    Imports: `from dataclasses import dataclass`, `from enum import Enum`.

    Top-of-file docstring: "Per D-D4, every Issue MUST have a non-UNKNOWN category in steady state. Persistent UNKNOWN issues are a system debt — see RESEARCH.md and CONTEXT.md."
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "
from polyarb.validator.category import Category, Issue
assert Category.ZOMBIE_MARKET.value == 'zombie_market'
assert Category.GHOST_BOOK.value == 'ghost_book'
assert Category.UNKNOWN.value == 'unknown'
# (str, Enum) mixin: equals string
assert Category.ZOMBIE_MARKET == 'zombie_market'
i = Issue(layer=1, category=Category.API_JITTER, market_id=None, detail='x')
assert i.layer == 1 and i.raw_payload is None
print('CATEGORY_OK')
"</automated>
  </verify>
  <done>category.py exports Category enum (7 members incl. GHOST_BOOK and UNKNOWN) with (str, Enum) mixin and frozen Issue dataclass; (str, Enum) mixin verified by `==` against literal string</done>
</task>

<task type="auto">
  <id>T5</id>
  <name>Task 5: Implement validator/layers.py (Layer 1 + 2 + 4 + is_valid_overall)</name>
  <files>src/polyarb/validator/layers.py</files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Pattern 5 lines 542-597 — three layers; Pitfall 1 lines 631-639 — ghost-book defense)
    - .planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md (D-D1, D-D2, D-D4 — Layer scope and category requirement)
    - src/polyarb/validator/category.py (T4)
  </read_first>
  <action>
    Create `src/polyarb/validator/layers.py` exporting four pure functions:

    Imports: `import json`, `from typing import Any`, `from polyarb.validator.category import Category, Issue`.

    Module-level constant:
    ```python
    REQUIRED_FIELDS = ("market_id", "condition_id", "yes_token_id", "no_token_id",
                       "mid_price", "liquidity_usd", "end_time_ms")
    GHOST_BOOK_ASK_THRESHOLD = 0.98
    GHOST_BOOK_BID_THRESHOLD = 0.02
    GHOST_BOOK_PRICE_DIVERGENCE = 0.05
    ZOMBIE_LIQUIDITY_USD = 10.0
    RESOLVING_WINDOW_MS = 24 * 60 * 60 * 1000  # 24h
    ```

    1. `def layer1_count(reported_total: int, fetched_count: int) -> list[Issue]`:
       - If `reported_total != fetched_count`: return one Issue with layer=1, category=Category.API_JITTER, market_id=None, detail=f"Gamma reported {reported_total} active markets, fetched {fetched_count}"
       - Else: return []

    2. `def layer2_fields(markets: list[dict], *, now_ms: int) -> list[Issue]`:
       - For each market dict: check each REQUIRED_FIELDS key is present and non-empty (None / "" / [] count as missing)
       - If any missing:
         - Categorize by heuristic:
           - If `end_time_ms` set AND `(end_time_ms - now_ms) < RESOLVING_WINDOW_MS` → `RESOLVING`
           - Else if `liquidity_usd is not None AND liquidity_usd < ZOMBIE_LIQUIDITY_USD` → `ZOMBIE_MARKET`
           - Else → `UNKNOWN`
         - Append Issue(layer=2, category=cat, market_id=m.get("market_id"), detail=f"missing: {missing_list}"[:200], raw_payload=json.dumps({k: m.get(k) for k in REQUIRED_FIELDS}, default=str)[:1024])
         - **F-5 SECURITY**: `raw_payload` truncated to 1024 bytes; `detail` to 200 bytes. A market with a 10MB `question` field would otherwise inflate `validation_issues` without bound.
         - SIDE EFFECT: Set `m["incomplete"] = True` (per RESEARCH.md Pattern 5 line 565 — mark, don't drop)
       - Return list of issues

    3. `def layer4_cross_source(markets: list[dict], books_by_token: dict[str, dict], prices_by_token: dict[str, Any]) -> list[Issue]`:
       - For each market m, for each token field in `("yes_token_id", "no_token_id")`:
         - `tid = m.get(token_field)`; skip if not tid
         - If `tid not in books_by_token`: append Issue(layer=4, category=CLOB_MISSING, market_id=m["market_id"], detail=f"CLOB has no book for {token_field}={tid}")
         - Else: ghost-book check (per RESEARCH.md Pitfall 1):
           - `book = books_by_token[tid]`
           - `asks = book.get("asks") or []`; `bids = book.get("bids") or []`
           - **F-1 SECURITY**: `float()` on attacker-controlled CLOB fields must not crash the validator.
             Wrap each coercion in try/except `(KeyError, TypeError, ValueError, IndexError)`:
             ```python
             def _safe_float(v):
                 try:
                     return float(v)
                 except (KeyError, TypeError, ValueError):
                     return None
             top_ask_price = _safe_float(asks[0].get("price")) if asks else None
             top_bid_price = _safe_float(bids[0].get("price")) if bids else None
             ```
             If `asks` is non-empty but coercion returns None, append Issue(layer=4, category=UNKNOWN, market_id=m["market_id"], detail=f"unparseable ask for {tid}", raw_payload=json.dumps(book, default=str)[:500]) and continue (don't run ghost-book check on unparseable book).
           - `ref = prices_by_token.get(tid)`; if `ref` is dict, take `ref.get("buy")` else assume scalar
           - If `top_ask_price is not None AND top_bid_price is not None AND top_ask_price > GHOST_BOOK_ASK_THRESHOLD AND top_bid_price < GHOST_BOOK_BID_THRESHOLD AND ref is not None`:
             - `ref_val = _safe_float(ref)`; if `ref_val is None`: skip ghost-book check (no reference to compare)
             - If `ref_val is not None AND abs(ref_val - top_ask_price) > GHOST_BOOK_PRICE_DIVERGENCE`:
               - Append Issue(layer=4, category=GHOST_BOOK, market_id=m["market_id"], detail=f"book bid={top_bid_price}/ask={top_ask_price} but /price={ref_val}")
       - Return list

    4. `def is_valid_overall(issues: list[Issue]) -> bool`:
       - Per resolved Q5: any Layer 1 issue → False; Layer 2/4 issues recorded but ignored for is_valid
       - `return not any(i.layer == 1 for i in issues)`

    Add a module top docstring: "Phase 1 validity policy (resolved Q5): Layer 1 strict (mismatch → invalid). Layer 2/4 = record-only. To be revisited after Phase 3 collects evidence on issue rates."

    NO logging in this module (pure functions). NO IO. NO async.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "
from polyarb.validator.layers import layer1_count, layer2_fields, layer4_cross_source, is_valid_overall
from polyarb.validator.category import Category
# Layer 1 mismatch
issues = layer1_count(100, 99)
assert len(issues) == 1 and issues[0].layer == 1 and issues[0].category == Category.API_JITTER
# Layer 1 match
assert layer1_count(100, 100) == []
# is_valid: Layer 1 fails
assert is_valid_overall(issues) is False
# is_valid: only Layer 2/4 → True
from polyarb.validator.category import Issue
ok_issues = [Issue(layer=2, category=Category.ZOMBIE_MARKET, market_id='m1', detail='x')]
assert is_valid_overall(ok_issues) is True
print('VALIDATOR_OK')
"</automated>
  </verify>
  <done>layers.py exports 4 functions; layer1_count returns single API_JITTER issue on mismatch; layer2_fields mutates incomplete=True and categorizes by heuristic; layer4_cross_source detects ghost books per issue #180; is_valid_overall returns False only when Layer 1 issue present</done>
</task>

<task type="auto">
  <id>T6</id>
  <name>Task 6: Unit tests for SQLiteStore (atomic replace + transaction safety)</name>
  <files>tests/m1-perception/test_sqlite_store.py</files>
  <read_first>
    - src/polyarb/storage/sqlite_store.py (T2)
    - src/polyarb/storage/schemas.py (T1)
    - src/polyarb/validator/category.py (T4)
  </read_first>
  <action>
    Create `tests/m1-perception/test_sqlite_store.py` with tests:

    Helper at top (use `pytest` fixture):
    ```python
    import sqlite3
    from pathlib import Path
    import pytest
    from polyarb.storage.sqlite_store import SQLiteStore
    from polyarb.validator.category import Category, Issue

    def make_market(market_id: str, **overrides) -> dict:
        base = dict(market_id=market_id, condition_id=f"c-{market_id}", slug=None,
                    question=None, yes_token_id="1"*70, no_token_id="2"*70,
                    mid_price=0.5, liquidity_usd=1000.0, volume_usd=100.0,
                    best_bid_price=0.49, best_bid_size=100.0,
                    best_ask_price=0.51, best_ask_size=100.0,
                    end_time_ms=2000000000000, active=1, closed=0,
                    neg_risk=0, neg_risk_market_id=None,
                    fetched_at_ms=1714435200000, snapshot_id=0, incomplete=0)
        base.update(overrides)
        return base

    @pytest.fixture
    def store(tmp_path):
        s = SQLiteStore(tmp_path / "t.db")
        s.init_schema()
        return s
    ```

    Tests:

    1. `test_init_schema_creates_three_tables` — call `init_schema()`; query `sqlite_master` for table names; assert {snapshots, markets, validation_issues} subset; assert WAL pragma is set (`PRAGMA journal_mode` returns 'wal')

    2. `test_init_schema_idempotent` — call init_schema twice; no exception; tables still 3

    3. `test_write_snapshot_overwrites_markets` — write snapshot 1 with [make_market("a"), make_market("b")]; write snapshot 2 with [make_market("c")]; query markets; assert exactly 1 row with market_id="c" (NOT "a" + "b" + "c" — that would be the INSERT OR REPLACE anti-pattern)

    4. `test_write_snapshot_appends_to_snapshots_table` — write 2 snapshots; query `SELECT COUNT(*) FROM snapshots`; assert 2

    5. `test_write_snapshot_records_issues_with_category` — write snapshot with issues=[Issue(2, Category.ZOMBIE_MARKET, "m1", "low liq"), Issue(4, Category.GHOST_BOOK, "m2", "fake book")]; query `SELECT category, layer FROM validation_issues ORDER BY layer`; assert [("zombie_market", 2), ("ghost_book", 4)]

    6. `test_write_snapshot_invalid_still_persists` — write snapshot with is_valid=False, market_rows=[make_market("a")], issues=[]; query `SELECT is_valid, market_count FROM snapshots`; assert (0, 1) — D-D3 confirmed

    7. `test_write_snapshot_returns_snapshot_id` — write snapshot; assert returned id is int >= 1; assert markets row has matching snapshot_id

    8. `test_write_snapshot_invalid_mode_raises` — call write_snapshot(mode="weekly", ...); assert ValueError

    9. `test_token_ids_preserve_uint256_string` — write market with yes_token_id="1"*70 (70-char numeric string); query back; assert exact string equality (Pitfall 3)

    10. `test_rollback_on_executemany_failure` — pass market_rows containing one row missing required column; assert exception raises; query markets; assert table is empty (rollback worked)
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception/test_sqlite_store.py -xvs 2>&1 | tail -40</automated>
  </verify>
  <done>All 10 tests pass; overwrite semantics verified (test 3); is_valid=False persistence verified (test 6); uint256 token_id preserved (test 9); rollback verified (test 10)</done>
</task>

<task type="auto">
  <id>T7</id>
  <name>Task 7: Unit tests for parquet_writer (path format + atomic write + schema)</name>
  <files>tests/m1-perception/test_parquet_writer.py</files>
  <read_first>
    - src/polyarb/storage/parquet_writer.py (T3)
    - src/polyarb/storage/schemas.py (T1)
  </read_first>
  <action>
    Create `tests/m1-perception/test_parquet_writer.py`:

    Imports: `import os`, `from pathlib import Path`, `import pytest`, `import pyarrow.parquet as pq`, `from polyarb.storage.parquet_writer import compute_snapshot_path, write_parquet_atomic`.

    Reuse `make_market_for_parquet(...)` helper (similar to T6 but with the 2 extra parquet-only fields: `snapshot_taken_at_ms`, `snapshot_id`; bool fields stay as Python bool, not int).

    Tests:

    1. `test_compute_snapshot_path_format` — compute path for `taken_at_ms = 1714435200000` (2024-04-30T00:00:00Z); assert path matches `<root>/2024/04/30/00-00-00.parquet`

    2. `test_compute_snapshot_path_uses_utc` — taken_at corresponding to a known UTC vs local time difference; assert UTC components used, not local

    3. `test_write_parquet_creates_file` — write 3 rows; assert out_path exists; pq.read_table(out_path).num_rows == 3

    4. `test_write_parquet_token_id_preserved_as_string` — write a row with yes_token_id="1"*70; read back via `pq.read_table(out_path).to_pylist()`; assert the value is the exact 70-char string (Pitfall 3 — would be silently corrupted to scientific notation if schema were not pa.string())

    5. `test_write_parquet_atomic_no_partial_file_on_failure` — write with rows that violate SNAPSHOT_SCHEMA (e.g., int where string expected); assert exception; assert `out_path` does NOT exist; assert no `.tmp` files remain in parent dir

    6. `test_write_parquet_creates_parent_dirs` — out_path = tmp_path/"a"/"b"/"c"/"file.parquet" (none of a/b/c exist); call write; assert out_path exists and parents were created

    7. `test_write_parquet_uses_snappy_compression` — write file; read via `pq.ParquetFile(out_path).metadata.row_group(0).column(0).compression`; assert it equals "SNAPPY"
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception/test_parquet_writer.py -xvs 2>&1 | tail -30</automated>
  </verify>
  <done>All 7 tests pass; path format correct; token_id preserved as string; atomic write verified (no partial file on failure); snappy compression confirmed</done>
</task>

<task type="auto">
  <id>T8</id>
  <name>Task 8: Unit tests for validator (Layer 1 strict + Layer 2 categorize + Layer 4 ghost-book)</name>
  <files>tests/m1-perception/test_validator.py</files>
  <read_first>
    - src/polyarb/validator/layers.py (T5)
    - src/polyarb/validator/category.py (T4)
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Pitfall 1 — ghost-book detection algorithm)
  </read_first>
  <action>
    Create `tests/m1-perception/test_validator.py`:

    Imports + helpers: `from polyarb.validator.layers import layer1_count, layer2_fields, layer4_cross_source, is_valid_overall`, `from polyarb.validator.category import Category, Issue`.

    Tests:

    Layer 1 (count):
    1. `test_layer1_match_no_issues` — `layer1_count(100, 100)` returns []
    2. `test_layer1_mismatch_returns_api_jitter` — `layer1_count(100, 99)` returns 1 issue with layer=1, category=API_JITTER, market_id=None
    3. `test_layer1_overshoot_also_flags` — `layer1_count(100, 101)` returns 1 issue (mismatch is symmetric)

    Layer 2 (fields):
    4. `test_layer2_complete_market_no_issue` — pass a market with all REQUIRED_FIELDS populated; assert []
    5. `test_layer2_missing_field_marks_incomplete_and_categorizes_zombie` — pass market with `mid_price=None`, liquidity_usd=5 (below ZOMBIE_LIQUIDITY_USD=10); assert 1 issue category=ZOMBIE_MARKET; assert mutated `m["incomplete"] is True`
    6. `test_layer2_missing_field_categorizes_resolving_when_endtime_near` — market with `mid_price=None`, end_time_ms = now_ms + 1_000_000 (< 24h); assert category=RESOLVING
    7. `test_layer2_missing_field_categorizes_unknown_when_no_heuristic_matches` — market with `mid_price=None`, liquidity_usd=10000, end_time_ms = now_ms + 365_days_ms; assert category=UNKNOWN
    8. `test_layer2_does_not_drop_market` — market list of 1 with missing field; after layer2 returns issues, the market dict still exists in the input list with `incomplete=True`

    Layer 4 (cross-source):
    9. `test_layer4_clob_missing_when_no_book` — market with yes_token_id="t1"; books_by_token={}; assert 1 issue category=CLOB_MISSING for the token
    10. `test_layer4_no_issue_when_book_present_and_normal_prices` — market with token "t1"; books_by_token={"t1": {"asks":[{"price":"0.55"}], "bids":[{"price":"0.45"}]}}; prices_by_token={"t1": {"buy":"0.55"}}; assert []
    11. `test_layer4_ghost_book_detected` — books_by_token={"t1": {"asks":[{"price":"0.99"}], "bids":[{"price":"0.01"}]}}; prices_by_token={"t1": {"buy":"0.55"}}; assert 1 issue category=GHOST_BOOK with detail mentioning the prices
    12. `test_layer4_no_ghost_when_prices_agree` — books bid=0.01 ask=0.99 BUT prices_by_token={"t1": {"buy":"0.99"}} (within 0.05 divergence); assert no GHOST_BOOK issue
    13. `test_layer4_handles_missing_prices_reference_gracefully` — books match ghost shape but prices_by_token={"t1": None}; assert no exception; no GHOST_BOOK issue (cannot detect without ground truth)
    14. `test_layer4_handles_two_tokens_per_market` — market with both yes_token_id and no_token_id; one missing in books → exactly 1 CLOB_MISSING issue (the one missing)

    is_valid_overall:
    15. `test_is_valid_true_when_no_issues` — `is_valid_overall([])` is True
    16. `test_is_valid_false_when_layer1_issue` — list with 1 Layer 1 issue → False
    17. `test_is_valid_true_when_only_layer2_4_issues` — list with mix of Layer 2 + Layer 4 issues → True (resolved Q5: no threshold this phase)

    Helper for Layer 2 tests: pass `now_ms = 1_714_435_200_000` (fixed timestamp for determinism).
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception/test_validator.py -xvs 2>&1 | tail -50</automated>
  </verify>
  <done>All 17 tests pass; Layer 1 strict equality verified; Layer 2 categorization heuristic verified for all 3 branches (zombie/resolving/unknown); Layer 4 ghost-book detection verified including the no-divergence + no-reference cases; is_valid_overall behavior matches resolved Q5</done>
</task>

</tasks>

## Verification

```bash
pytest tests/m1-perception/test_sqlite_store.py tests/m1-perception/test_parquet_writer.py tests/m1-perception/test_validator.py -xvs
python -c "from polyarb.storage.schemas import DDL, SNAPSHOT_SCHEMA; from polyarb.storage.sqlite_store import SQLiteStore; from polyarb.storage.parquet_writer import write_parquet_atomic, compute_snapshot_path; from polyarb.validator.layers import layer1_count, layer2_fields, layer4_cross_source, is_valid_overall; from polyarb.validator.category import Category, Issue; print('STORAGE_VALIDATOR_OK')"
```

## Success Criteria

- All 8 source/test files exist
- ≥34 tests pass (10 store + 7 parquet + 17 validator)
- Schemas stay column-aligned (DDL columns ↔ pa.Schema fields ↔ MARKETS_COLUMN_ORDER)
- SQLite overwrite semantics verified (no leftover rows from previous snapshots)
- Parquet atomicity verified (no partial files on failure)
- Ghost-book detection (issue #180) implemented in Layer 4 with both positive (detect) and negative (no false positive when prices agree) tests

## must_haves (this plan delivers)

- Phase outcomes 3, 4, 5, 6, 7 (storage atomicity, parquet schema, validator layers, ghost-book detection, fetched_at_ms column)

<output>
Create `.planning/workstreams/m1-perception/phases/01-/01-3-SUMMARY.md` documenting:
- Final list of column names in MARKETS_COLUMN_ORDER (so Plan 4 normalizer knows the contract)
- Confirmed `is_valid` policy (Layer 1 strict, Layer 2/4 record-only — resolved Q5)
- Whether any test required adjustment to the layer2 categorization heuristic
- Sample SQLite query to debug a snapshot (e.g., `SELECT category, COUNT(*) FROM validation_issues WHERE snapshot_id = (SELECT MAX(id) FROM snapshots) GROUP BY category`)
</output>
