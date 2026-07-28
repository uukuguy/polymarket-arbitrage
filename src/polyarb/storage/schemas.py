"""Storage schemas: SQLite DDL + pyarrow.Schema (column-aligned).

Phase 1.1 Amendment 01 (2026-05-02):
    Wave-1 Step-4 schema 验证证伪了 "Gamma /markets returns category/tags".
    实际：那俩字段只在 /events 上。Path C 决议：抓 /events 建 events + event_tags
    表，删 markets 的 category/tags 列改用 event_id 外键。

Phase 02 Plan 01 (2026-05-12):
    Added page_fetched_at_ms (nullable INTEGER) to markets and events tables.
    Semantic note (Phase 02): fetched_at_ms is the STAGE 5 completion stamp,
    same value for all rows within a snapshot. Use page_fetched_at_ms for per-page
    real fetch time (nullable for pre-02 snapshots). See L2 in LEARNINGS.

MARKETS_COLUMN_ORDER, MARKETS_INSERT_SQL, and SNAPSHOT_SCHEMA must stay in lockstep.
Adding/removing a column requires updating ALL FOUR sync points (DDL/MARKETS_COLUMN_ORDER/
MARKETS_INSERT_SQL/SNAPSHOT_SCHEMA). Phase 1.1 added a fifth sync point: the
question_translations table DDL block must exist for translation cache CRUD to
work. Amendment 01 added two more tables (events / event_tags) with their own
INSERT_SQL constants — these are SQLite-only (NOT in SNAPSHOT_SCHEMA, parquet
不存 events).

Tables:
- snapshots             — append-only metadata per snapshot run
- events                — Polymarket /events rows (per snapshot, not append-only)
- event_tags            — many-to-many event→tag, PK (event_id, tag_id, snapshot_id)
- markets               — atomically overwritten on each snapshot (D-C1); FK event_id
- validation_issues     — categorized validation failures per snapshot
- snapshot_source_coverage — completion proof for every snapshot attempt
- event_market_memberships — structural event membership per snapshot
- neg_risk_group_truth  — classified, hashed neg-risk group truth per snapshot
- question_translations — append-only translation cache (T2, never DELETE FROM)

The pyarrow SNAPSHOT_SCHEMA mirrors the markets table plus 1 parquet-only field
(snapshot_taken_at_ms). Token IDs are pa.string() because Polymarket
uint256 token IDs (70+ decimal digits) overflow pa.int64() — see RESEARCH.md Pitfall 3.

Events / event_tags are NOT serialized to parquet — they are a SQLite-only relational
view used for tag-based query and event-level aggregation; users wanting cross-snapshot
event analysis can re-derive from the SQLite db directly.
"""

from __future__ import annotations

import pyarrow as pa

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS snapshots (
  id                    INTEGER PRIMARY KEY AUTOINCREMENT,
  taken_at_ms           INTEGER NOT NULL,
  finished_at_ms        INTEGER NOT NULL,
  mode                  TEXT NOT NULL CHECK(mode IN ('subset','full')),
  market_count          INTEGER NOT NULL,
  market_view_published INTEGER NOT NULL DEFAULT 0 CHECK(market_view_published IN (0,1)),
  -- Product identity prevents historical combined CLOB snapshots from being
  -- mistaken for a current certified Structure revision after the split.
  data_product          TEXT NOT NULL DEFAULT 'legacy_combined',
  archive_status        TEXT NOT NULL DEFAULT 'legacy',
  snapshot_status       TEXT NOT NULL DEFAULT 'ok',
  is_valid              INTEGER NOT NULL,
  parquet_path          TEXT NOT NULL,
  notes                 TEXT,
  -- Phase 02 Plan 03: post-write mirror + r2 tracking; nullable for pre-02 snapshots
  -- and during transitional deploys (Supabase/R2 disabled).
  supabase_mirror_at_ms INTEGER,  -- ms timestamp of last successful Supabase mirror push
  parquet_r2_url        TEXT      -- R2 archive URL once upload_parquet_to_r2 succeeds
);

-- Phase 1.1 Amendment 01: events table fed from Gamma /events endpoint.
-- Each snapshot's events are inserted with its snapshot_id; events are NOT
-- DELETE FROM-overwritten like markets — they are append-only-per-snapshot
-- so we can join historical markets back to their event metadata.
CREATE TABLE IF NOT EXISTS events (
  id              TEXT NOT NULL,
  slug            TEXT NOT NULL,
  title           TEXT,
  ticker          TEXT,
  active          INTEGER NOT NULL DEFAULT 1,
  closed          INTEGER NOT NULL DEFAULT 0,
  liquidity_usd   REAL,
  volume_usd      REAL,
  end_time_ms     INTEGER,
  -- Semantic note (Phase 02): fetched_at_ms is the STAGE 5 completion stamp,
  -- same value for all rows within a snapshot. Use page_fetched_at_ms for per-page
  -- real fetch time (nullable for pre-02 snapshots).
  fetched_at_ms   INTEGER NOT NULL,
  page_fetched_at_ms INTEGER,
  snapshot_id     INTEGER NOT NULL REFERENCES snapshots(id),
  PRIMARY KEY (id, snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_events_slug ON events(slug);
CREATE INDEX IF NOT EXISTS idx_events_snapshot ON events(snapshot_id);

-- Phase 1.1 Amendment 01: many-to-many event→tag relation.
-- tag_id is Polymarket's stable tag identifier; tag_label is the human-readable
-- form ("Finance" / "Crypto" / "AL West" / etc.); tag_slug is the URL slug form.
CREATE TABLE IF NOT EXISTS event_tags (
  event_id    TEXT NOT NULL,
  tag_id      TEXT NOT NULL,
  tag_label   TEXT NOT NULL,
  tag_slug    TEXT NOT NULL,
  snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  PRIMARY KEY (event_id, tag_id, snapshot_id)
);

CREATE INDEX IF NOT EXISTS idx_event_tags_label ON event_tags(tag_label);
CREATE INDEX IF NOT EXISTS idx_event_tags_slug ON event_tags(tag_slug);
CREATE INDEX IF NOT EXISTS idx_event_tags_snapshot ON event_tags(snapshot_id);

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
  -- Semantic note (Phase 02): fetched_at_ms is the STAGE 5 completion stamp,
  -- same value for all rows within a snapshot. Use page_fetched_at_ms for per-page
  -- real fetch time (nullable for pre-02 snapshots).
  fetched_at_ms      INTEGER NOT NULL,
  page_fetched_at_ms INTEGER,
  snapshot_id        INTEGER NOT NULL REFERENCES snapshots(id),
  incomplete         INTEGER NOT NULL DEFAULT 0,
  event_id           TEXT
);

CREATE INDEX IF NOT EXISTS idx_markets_liquidity ON markets(liquidity_usd);
CREATE INDEX IF NOT EXISTS idx_markets_end_time ON markets(end_time_ms);
CREATE INDEX IF NOT EXISTS idx_markets_event_id ON markets(event_id);

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

-- Verified market-truth publication metadata. A row exists for every snapshot
-- attempt, including diagnostics that were not allowed to replace `markets`.
CREATE TABLE IF NOT EXISTS snapshot_source_coverage (
  snapshot_id INTEGER PRIMARY KEY REFERENCES snapshots(id),
  completed INTEGER NOT NULL CHECK(completed IN (0,1)),
  market_items INTEGER NOT NULL CHECK(market_items >= 0),
  event_items INTEGER NOT NULL CHECK(event_items >= 0),
  failure_source TEXT,
  failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS event_market_memberships (
  snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  event_id TEXT NOT NULL,
  neg_risk_market_id TEXT NOT NULL,
  market_id TEXT NOT NULL,
  member_kind TEXT NOT NULL CHECK(member_kind IN ('named','other','inactive-reserved')),
  active INTEGER NOT NULL CHECK(active IN (0,1)),
  closed INTEGER NOT NULL CHECK(closed IN (0,1)),
  PRIMARY KEY(snapshot_id, event_id, market_id)
);

CREATE TABLE IF NOT EXISTS neg_risk_group_truth (
  snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  event_id TEXT NOT NULL,
  neg_risk_market_id TEXT NOT NULL,
  neg_risk_type TEXT NOT NULL CHECK(neg_risk_type IN ('standard','augmented')),
  expected_member_count INTEGER NOT NULL CHECK(expected_member_count >= 0),
  active_named_count INTEGER NOT NULL CHECK(active_named_count >= 0),
  membership_hash TEXT NOT NULL,
  quality TEXT NOT NULL CHECK(quality IN (
    'complete-supported','complete-unsupported','incomplete-source','incomplete-quotes'
  )),
  reason TEXT,
  PRIMARY KEY(snapshot_id, neg_risk_market_id),
  CHECK (expected_member_count > 0 OR quality = 'incomplete-source')
);

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

-- H-009 Task 1: an atomic, read-only CLOB quote-run sidecar. Snapshot writes
-- continue to own `snapshots`/`markets`; this schema only records the versioned
-- membership that a quote run was asked to cover and its terminal observations.
CREATE TABLE IF NOT EXISTS neg_risk_quote_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  universe_snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  universe_taken_at_ms INTEGER NOT NULL,
  universe_hash TEXT NOT NULL DEFAULT '',
  source_truth_hash TEXT NOT NULL DEFAULT '',
  quoted_at_ms INTEGER NOT NULL,
  requested_token_count INTEGER NOT NULL CHECK(requested_token_count >= 0),
  successful_response_count INTEGER NOT NULL DEFAULT 0
      CHECK(successful_response_count >= 0),
  -- A collecting run owns the quote producer only until this timestamp.  A
  -- crashed process stops renewing it, allowing the next BEGIN IMMEDIATE to
  -- recover the run without ever taking a live collector's lease.
  lease_expires_at_ms INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL CHECK(status IN ('collecting', 'complete', 'failed')),
  failure_reason TEXT,
  completed_at_ms INTEGER,
  CHECK((status = 'complete' AND failure_reason IS NULL AND completed_at_ms IS NOT NULL)
     OR (status = 'failed' AND failure_reason IS NOT NULL)
     OR (status = 'collecting' AND failure_reason IS NULL AND completed_at_ms IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_quote_runs_select
  ON neg_risk_quote_runs(status, quoted_at_ms DESC, id DESC);

-- The requested set must be durable so independently-created store instances
-- can reject quote rows for tokens outside their run's snapshot universe.
CREATE TABLE IF NOT EXISTS neg_risk_quote_run_legs (
  quote_run_id INTEGER NOT NULL REFERENCES neg_risk_quote_runs(id),
  neg_risk_market_id TEXT NOT NULL,
  event_id TEXT NOT NULL DEFAULT '',
  membership_hash TEXT NOT NULL DEFAULT '',
  market_id TEXT NOT NULL,
  condition_id TEXT NOT NULL,
  slug TEXT,
  yes_token_id TEXT NOT NULL,
  PRIMARY KEY(quote_run_id, yes_token_id)
);

CREATE TABLE IF NOT EXISTS neg_risk_quotes (
  quote_run_id INTEGER NOT NULL REFERENCES neg_risk_quote_runs(id),
  neg_risk_market_id TEXT NOT NULL,
  event_id TEXT NOT NULL DEFAULT '',
  membership_hash TEXT NOT NULL DEFAULT '',
  market_id TEXT NOT NULL,
  condition_id TEXT NOT NULL,
  slug TEXT,
  yes_token_id TEXT NOT NULL,
  terminal_state TEXT NOT NULL CHECK(terminal_state IN (
    'executable', 'missing-book', 'missing-ask', 'invalid-ask-price',
    'invalid-ask-size', 'collector-error'
  )),
  best_ask_price REAL,
  best_ask_size REAL,
  PRIMARY KEY(quote_run_id, yes_token_id),
  CHECK((terminal_state = 'executable' AND best_ask_price > 0
      AND best_ask_price <= 1 AND best_ask_size > 0)
    OR (terminal_state != 'executable' AND best_ask_price IS NULL
      AND best_ask_size IS NULL))
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_quotes_run_group
  ON neg_risk_quotes(quote_run_id, neg_risk_market_id, market_id);

-- Observer-only lifecycle ledger. A master represents one continuously
-- observed Structure membership; observations and notification attempts are
-- immutable evidence beneath it.
CREATE TABLE IF NOT EXISTS neg_risk_opportunities (
  id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL,
  group_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('observe','closed','invalidated','unavailable')),
  bundle_cost REAL NOT NULL,
  gross_edge_bps REAL NOT NULL,
  max_bundle_size REAL NOT NULL,
  structure_revision INTEGER NOT NULL,
  quote_run_id INTEGER NOT NULL,
  opened_at_ms INTEGER NOT NULL,
  updated_at_ms INTEGER NOT NULL,
  closed_at_ms INTEGER,
  transition_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_neg_risk_opportunities_active_identity
  ON neg_risk_opportunities(event_id, group_id, membership_hash)
  WHERE status = 'observe';
CREATE INDEX IF NOT EXISTS idx_neg_risk_opportunities_current
  ON neg_risk_opportunities(status, updated_at_ms DESC);

CREATE TABLE IF NOT EXISTS neg_risk_opportunity_observations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  opportunity_id TEXT NOT NULL REFERENCES neg_risk_opportunities(id),
  observed_at_ms INTEGER NOT NULL,
  source TEXT NOT NULL CHECK(source IN ('global','focused')),
  status TEXT NOT NULL CHECK(status IN ('observe','closed','invalidated','unavailable')),
  reason TEXT,
  bundle_cost REAL,
  gross_edge_bps REAL,
  max_bundle_size REAL,
  structure_revision INTEGER NOT NULL,
  quote_run_id INTEGER,
  legs_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_opportunity_observations_replay
  ON neg_risk_opportunity_observations(opportunity_id, observed_at_ms, id);

CREATE TABLE IF NOT EXISTS neg_risk_opportunity_notifications (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  opportunity_id TEXT NOT NULL REFERENCES neg_risk_opportunities(id),
  reason TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('pending','delivered','failed')),
  attempt_count INTEGER NOT NULL DEFAULT 0 CHECK(attempt_count >= 0),
  created_at_ms INTEGER NOT NULL,
  attempted_at_ms INTEGER,
  delivered_at_ms INTEGER,
  error_kind TEXT
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_opportunity_notifications_pending
  ON neg_risk_opportunity_notifications(status, created_at_ms, id);

-- Notification rows are immutable delivery intents. Each transport result is
-- an append-only attempt so retry state can be derived without erasing prior
-- failure or delivery evidence from deployed databases.
CREATE TABLE IF NOT EXISTS neg_risk_opportunity_notification_attempts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  notification_id INTEGER NOT NULL REFERENCES neg_risk_opportunity_notifications(id),
  attempted_at_ms INTEGER NOT NULL,
  outcome TEXT NOT NULL CHECK(outcome IN ('delivered','failed')),
  error_kind TEXT
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_opportunity_notification_attempts_replay
  ON neg_risk_opportunity_notification_attempts(notification_id, attempted_at_ms, id);
"""

# Order MUST match the DDL `CREATE TABLE markets(...)` declaration
# AND the placeholders in MARKETS_INSERT_SQL.
# Semantic note (Phase 02): fetched_at_ms is the STAGE 5 completion stamp,
# same value for all rows within a snapshot. Use page_fetched_at_ms for per-page
# real fetch time (nullable for pre-02 snapshots).
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
    "page_fetched_at_ms",  # Phase 02 Plan 01: per-page real fetch time (nullable, fixes L2)
    "snapshot_id",
    "incomplete",
    "event_id",  # Phase 1.1 Amendment 01: FK to events(id)
)

MARKETS_INSERT_SQL = (
    "INSERT INTO markets("
    "market_id,condition_id,slug,question,yes_token_id,no_token_id,"
    "mid_price,liquidity_usd,volume_usd,best_bid_price,best_bid_size,best_ask_price,"
    "best_ask_size,end_time_ms,active,closed,neg_risk,neg_risk_market_id,"
    "fetched_at_ms,page_fetched_at_ms,snapshot_id,incomplete,event_id) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)

# ─────────────────────────────────────────────────────────────────────────────
# Phase 1.1 Amendment 01: events / event_tags column orders + INSERT SQL
# ─────────────────────────────────────────────────────────────────────────────

EVENTS_COLUMN_ORDER: tuple[str, ...] = (
    "id",
    "slug",
    "title",
    "ticker",
    "active",
    "closed",
    "liquidity_usd",
    "volume_usd",
    "end_time_ms",
    # Semantic note (Phase 02): fetched_at_ms is the STAGE 5 completion stamp,
    # same value for all rows within a snapshot. Use page_fetched_at_ms for per-page
    # real fetch time (nullable for pre-02 snapshots).
    "fetched_at_ms",
    "page_fetched_at_ms",  # Phase 02 Plan 01: per-page real fetch time (nullable, fixes L2)
    "snapshot_id",
)

EVENTS_INSERT_SQL = (
    "INSERT INTO events("
    "id,slug,title,ticker,active,closed,liquidity_usd,volume_usd,"
    "end_time_ms,fetched_at_ms,page_fetched_at_ms,snapshot_id) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
)

EVENT_TAGS_COLUMN_ORDER: tuple[str, ...] = (
    "event_id",
    "tag_id",
    "tag_label",
    "tag_slug",
    "snapshot_id",
)

EVENT_TAGS_INSERT_SQL = (
    "INSERT INTO event_tags(event_id,tag_id,tag_label,tag_slug,snapshot_id) VALUES (?,?,?,?,?)"
)

# Pyarrow schema for Parquet — token IDs are pa.string() (Pitfall 3: uint256 > int64).
# bool fields are pa.bool_() in Parquet but stored as INTEGER in SQLite; the writer
# is responsible for the bool↔int translation per side.
#
# NOTE (Amendment 01): events / event_tags are SQLite-only and NOT serialized
# to parquet. This is by design — parquet is the historical market snapshot
# wire format; event metadata is denormalized via the event_id FK on markets,
# which IS in the parquet (so cross-snapshot tag analysis can rejoin via SQLite).
# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 02: scheduler_state singleton table
#
# A single-row table that persists the scheduler's state across restarts.
# CHECK (id = 1) enforces the singleton invariant (only row 0 ever exists).
# No parquet mirror — this is scheduler metadata, not market data.
# NOT in SNAPSHOT_SCHEMA (parquet doesn't include scheduler state).
# ─────────────────────────────────────────────────────────────────────────────

SCHEDULER_STATE_DDL = """
CREATE TABLE IF NOT EXISTS scheduler_state (
    id               INTEGER PRIMARY KEY DEFAULT 1,
    state            TEXT NOT NULL,
    failure_counter  INTEGER NOT NULL DEFAULT 0,
    updated_at_ms    INTEGER NOT NULL,
    CHECK (id = 1)
)
"""

# Append-only parent-process evidence for scheduler-launched snapshot children.
# A child killed by the kernel cannot write a terminal snapshots row; this table
# records the parent-observed result without changing market publication truth.
SNAPSHOT_ATTEMPTS_DDL = """
CREATE TABLE IF NOT EXISTS snapshot_attempts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at_ms  INTEGER NOT NULL,
    finished_at_ms INTEGER,
    outcome        TEXT NOT NULL CHECK(outcome IN ('running','succeeded','failed','cancelled')),
    snapshot_id    INTEGER REFERENCES snapshots(id),
    failure_kind   TEXT,
    last_stage     TEXT,
    elapsed_ms     INTEGER,
    CHECK(
        (outcome = 'running' AND finished_at_ms IS NULL
         AND snapshot_id IS NULL AND failure_kind IS NULL)
        OR (outcome = 'succeeded' AND finished_at_ms IS NOT NULL
            AND snapshot_id IS NOT NULL AND failure_kind IS NULL)
        OR (outcome IN ('failed','cancelled') AND finished_at_ms IS NOT NULL)
    )
);
CREATE INDEX IF NOT EXISTS idx_snapshot_attempts_started_at_ms
ON snapshot_attempts(started_at_ms DESC);
"""

STRUCTURE_SCHEDULE_ADJUSTMENTS_DDL = """
CREATE TABLE IF NOT EXISTS structure_schedule_adjustments (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    source_attempt_id    INTEGER NOT NULL UNIQUE REFERENCES snapshot_attempts(id),
    decided_at_ms        INTEGER NOT NULL,
    success_sample_count INTEGER NOT NULL,
    success_p95_s        INTEGER,
    previous_timeout_s   INTEGER NOT NULL,
    previous_cadence_s   INTEGER NOT NULL,
    timeout_s            INTEGER NOT NULL,
    cadence_s            INTEGER NOT NULL,
    reason               TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_structure_schedule_adjustments_decided_at
ON structure_schedule_adjustments(decided_at_ms DESC);
"""

# ─────────────────────────────────────────────────────────────────────────────
# Phase 03.1 Plan 01: l2_mirror_state singleton table (GAP-2 + GAP-3)
#
# Local freshness cache: the L2 Supabase mirror writes here on every successful
# push (push_top_of_book / push_trades). /health l2_tob_age_seconds sub-check
# reads from this cache via SQLiteStore.get_l2_tob_last_mirror_at_s().
#
# Why a local cache instead of querying Supabase?
#   Sub-second /health probes MUST NOT round-trip to Supabase (latency + Free-tier
#   request budget). The cache is the freshness anchor referenced by Inj L2-2
#   RCA — Phase 03 mirror failure stayed silent because nothing surfaced
#   "last successful mirror at" to /health.
#
# Schema:
#   l2_mirror_state (
#       id               INTEGER PRIMARY KEY CHECK(id=1),
#       last_mirror_at_s INTEGER NOT NULL   -- wall-clock seconds since epoch
#   )
# ─────────────────────────────────────────────────────────────────────────────

L2_MIRROR_STATE_DDL = """
CREATE TABLE IF NOT EXISTS l2_mirror_state (
    id               INTEGER PRIMARY KEY CHECK(id=1),
    last_mirror_at_s INTEGER NOT NULL
)
"""

# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 03: snapshots table lockstep (3-point: DDL / COLUMN_ORDER / INSERT_SQL)
#
# The snapshots table is NOT in parquet (only markets is), so there are only
# 3 sync points vs the 4-point lockstep for markets. The new nullable columns
# supabase_mirror_at_ms + parquet_r2_url are added here for post-write tracking.
# NOTE: SNAPSHOTS_DDL is a string constant mirroring the DDL block above for
# test-assertion purposes only. The actual DDL is applied via the main DDL string.
# ─────────────────────────────────────────────────────────────────────────────

SNAPSHOTS_DDL = (
    "CREATE TABLE IF NOT EXISTS snapshots ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "taken_at_ms INTEGER NOT NULL, "
    "finished_at_ms INTEGER NOT NULL, "
    "mode TEXT NOT NULL, "
    "market_count INTEGER NOT NULL, "
    "market_view_published INTEGER NOT NULL DEFAULT 0, "
    "data_product TEXT NOT NULL DEFAULT 'legacy_combined', "
    "archive_status TEXT NOT NULL DEFAULT 'legacy', "
    "snapshot_status TEXT NOT NULL DEFAULT 'ok', "
    "is_valid INTEGER NOT NULL, "
    "parquet_path TEXT NOT NULL, "
    "notes TEXT, "
    "supabase_mirror_at_ms INTEGER, "  # Phase 02 Plan 03
    "parquet_r2_url TEXT"  # Phase 02 Plan 03
    ")"
)

SNAPSHOTS_COLUMN_ORDER: tuple[str, ...] = (
    "id",
    "taken_at_ms",
    "finished_at_ms",
    "mode",
    "market_count",
    "market_view_published",
    "data_product",
    "archive_status",
    "snapshot_status",
    "is_valid",
    "parquet_path",
    "notes",
    "supabase_mirror_at_ms",  # Phase 02 Plan 03: nullable mirror timestamp
    "parquet_r2_url",  # Phase 02 Plan 03: nullable R2 URL
)

SNAPSHOTS_INSERT_SQL = (
    "INSERT INTO snapshots("
    "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
    "data_product,archive_status,snapshot_status,is_valid,parquet_path,notes,"
    "supabase_mirror_at_ms,parquet_r2_url) "
    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
)

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
        # Semantic note (Phase 02): fetched_at_ms is the STAGE 5 completion stamp,
        # same value for all rows within a snapshot. Use page_fetched_at_ms for
        # per-page real fetch time (nullable for pre-02 snapshots).
        pa.field("fetched_at_ms", pa.int64()),
        pa.field("page_fetched_at_ms", pa.int64(), nullable=True),  # Phase 02 Plan 01: fixes L2
        pa.field("snapshot_taken_at_ms", pa.int64()),
        pa.field("snapshot_id", pa.int64()),
        pa.field("incomplete", pa.bool_()),
        pa.field("event_id", pa.string(), nullable=True),  # Phase 1.1 Amendment 01
    ]
)
