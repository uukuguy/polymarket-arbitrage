"""Storage schemas: SQLite DDL + pyarrow.Schema (column-aligned).

MARKETS_COLUMN_ORDER, MARKETS_INSERT_SQL, and SNAPSHOT_SCHEMA must stay in lockstep.
Adding a column requires updating ALL FOUR (DDL/MARKETS_COLUMN_ORDER/MARKETS_INSERT_SQL/
SNAPSHOT_SCHEMA). Phase 1.1 added a fifth sync point: the question_translations table
DDL block must exist for translation cache CRUD to work.

Tables:
- snapshots             — append-only metadata per snapshot run
- markets               — atomically overwritten on each snapshot (D-C1)
- validation_issues     — categorized validation failures per snapshot
- question_translations — append-only translation cache (T2, never DELETE FROM)

The pyarrow SNAPSHOT_SCHEMA mirrors the markets table plus 1 parquet-only field
(snapshot_taken_at_ms). Token IDs are pa.string() because Polymarket
uint256 token IDs (70+ decimal digits) overflow pa.int64() — see RESEARCH.md Pitfall 3.
"""

from __future__ import annotations

import pyarrow as pa

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS snapshots (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  taken_at_ms     INTEGER NOT NULL,
  finished_at_ms  INTEGER NOT NULL,
  mode            TEXT NOT NULL CHECK(mode IN ('subset','full')),
  market_count    INTEGER NOT NULL,
  is_valid        INTEGER NOT NULL,
  parquet_path    TEXT NOT NULL,
  notes           TEXT
);

CREATE TABLE IF NOT EXISTS markets (
  market_id          TEXT PRIMARY KEY,
  condition_id       TEXT NOT NULL,
  slug               TEXT,
  question           TEXT,
  yes_token_id       TEXT,
  no_token_id        TEXT,
  mid_price          REAL,
  liquidity_usd      REAL,
  volume_usd         REAL,
  best_bid_price     REAL,
  best_bid_size      REAL,
  best_ask_price     REAL,
  best_ask_size      REAL,
  end_time_ms        INTEGER,
  active             INTEGER,
  closed             INTEGER,
  neg_risk           INTEGER,
  neg_risk_market_id TEXT,
  fetched_at_ms      INTEGER NOT NULL,
  snapshot_id        INTEGER NOT NULL REFERENCES snapshots(id),
  incomplete         INTEGER NOT NULL DEFAULT 0,
  category           TEXT,
  tags               TEXT
);

CREATE INDEX IF NOT EXISTS idx_markets_liquidity ON markets(liquidity_usd);
CREATE INDEX IF NOT EXISTS idx_markets_end_time ON markets(end_time_ms);

CREATE TABLE IF NOT EXISTS validation_issues (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id  INTEGER NOT NULL REFERENCES snapshots(id),
  layer        INTEGER NOT NULL,
  category     TEXT NOT NULL,
  market_id    TEXT,
  detail       TEXT,
  raw_payload  TEXT
);

CREATE INDEX IF NOT EXISTS idx_issues_snapshot ON validation_issues(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_issues_category ON validation_issues(category);

-- Phase 1.1 T2: append-only translation cache.
-- Invariants:
--  * never DELETE FROM (cumulative across snapshots)
--  * question_hash = sha256(question_en) for de-dup
--  * is_dead=1 marks a question that exceeded retry_count > 3 (manual reset to retry)
--  * idx_qt_question_en UNIQUE protects scan-time LEFT JOIN ON m.question = qt.question_en
--    from producing duplicate rows.
CREATE TABLE IF NOT EXISTS question_translations (
  question_hash    TEXT PRIMARY KEY,
  question_en      TEXT NOT NULL,
  question_zh      TEXT NOT NULL,
  translator_model TEXT NOT NULL,
  translated_at_ms INTEGER NOT NULL,
  token_cost       INTEGER,
  retry_count      INTEGER NOT NULL DEFAULT 0,
  is_dead          INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_qt_dead ON question_translations(is_dead);
CREATE UNIQUE INDEX IF NOT EXISTS idx_qt_question_en ON question_translations(question_en);
"""

# Order MUST match the DDL `CREATE TABLE markets(...)` declaration
# AND the placeholders in MARKETS_INSERT_SQL.
MARKETS_COLUMN_ORDER: tuple[str, ...] = (
    "market_id",
    "condition_id",
    "slug",
    "question",
    "yes_token_id",
    "no_token_id",
    "mid_price",
    "liquidity_usd",
    "volume_usd",
    "best_bid_price",
    "best_bid_size",
    "best_ask_price",
    "best_ask_size",
    "end_time_ms",
    "active",
    "closed",
    "neg_risk",
    "neg_risk_market_id",
    "fetched_at_ms",
    "snapshot_id",
    "incomplete",
    "category",  # Phase 1.1 T1
    "tags",      # Phase 1.1 T1 (json.dumps(list[str]) string)
)

MARKETS_INSERT_SQL = (
    "INSERT INTO markets("
    "market_id,condition_id,slug,question,yes_token_id,no_token_id,"
    "mid_price,liquidity_usd,volume_usd,best_bid_price,best_bid_size,best_ask_price,"
    "best_ask_size,end_time_ms,active,closed,neg_risk,neg_risk_market_id,"
    "fetched_at_ms,snapshot_id,incomplete,category,tags) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)

# Pyarrow schema for Parquet — token IDs are pa.string() (Pitfall 3: uint256 > int64).
# bool fields are pa.bool_() in Parquet but stored as INTEGER in SQLite; the writer
# is responsible for the bool↔int translation per side.
SNAPSHOT_SCHEMA: pa.Schema = pa.schema(
    [
        pa.field("market_id", pa.string()),
        pa.field("condition_id", pa.string()),
        pa.field("slug", pa.string(), nullable=True),
        pa.field("question", pa.string(), nullable=True),
        pa.field("yes_token_id", pa.string(), nullable=True),
        pa.field("no_token_id", pa.string(), nullable=True),
        pa.field("mid_price", pa.float64(), nullable=True),
        pa.field("liquidity_usd", pa.float64(), nullable=True),
        pa.field("volume_usd", pa.float64(), nullable=True),
        pa.field("best_bid_price", pa.float64(), nullable=True),
        pa.field("best_bid_size", pa.float64(), nullable=True),
        pa.field("best_ask_price", pa.float64(), nullable=True),
        pa.field("best_ask_size", pa.float64(), nullable=True),
        pa.field("end_time_ms", pa.int64(), nullable=True),
        pa.field("active", pa.bool_()),
        pa.field("closed", pa.bool_()),
        pa.field("neg_risk", pa.bool_()),
        pa.field("neg_risk_market_id", pa.string(), nullable=True),
        pa.field("fetched_at_ms", pa.int64()),
        pa.field("snapshot_taken_at_ms", pa.int64()),
        pa.field("snapshot_id", pa.int64()),
        pa.field("incomplete", pa.bool_()),
        pa.field("category", pa.string(), nullable=True),  # Phase 1.1 T1
        pa.field("tags", pa.string(), nullable=True),      # Phase 1.1 T1
    ]
)
