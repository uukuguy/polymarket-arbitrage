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

# Exact pre-v2 owner guard accepted for the one supported a527 migration.
# Keep this as an explicit historical contract: startup migration must not
# depend on a git checkout or infer an old schema by subtracting columns.
A527_OWNER_MUTATION_GUARD_DDL = """
CREATE TABLE neg_risk_owner_mutation_guard (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  consumed_journal_id INTEGER NOT NULL CHECK(consumed_journal_id >= 0),
  consumed_hash TEXT,
  retained_base_id INTEGER NOT NULL DEFAULT 0 CHECK(retained_base_id >= 0),
  retained_base_hash TEXT,
  candidate_aggregate_hash TEXT,
  discovery_aggregate_hash TEXT)
"""

# Canonical v2 DDL is also explicit so the a527 migration can rebuild the
# table atomically with every constraint, rather than merely appending two
# nullable columns via ALTER TABLE.
OWNER_MUTATION_GUARD_DDL = """
CREATE TABLE neg_risk_owner_mutation_guard (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  consumed_journal_id INTEGER NOT NULL CHECK(consumed_journal_id >= 0),
  consumed_hash TEXT,
  retained_base_id INTEGER NOT NULL DEFAULT 0 CHECK(retained_base_id >= 0),
  retained_base_hash TEXT,
  candidate_aggregate_hash TEXT,
  discovery_aggregate_hash TEXT,
  authority_version INTEGER NOT NULL CHECK(authority_version = 2),
  migration_state TEXT NOT NULL CHECK(migration_state IN ('building','complete'))
)
"""

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

-- Opportunity-first read model. Group revisions are immutable membership
-- evidence; quote batches are published only against the certified membership
-- re-read inside the same SQLite write transaction.
CREATE TABLE IF NOT EXISTS neg_risk_group_revisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  membership_hash TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL,
  observed_at_ms INTEGER NOT NULL,
  source_cursor TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status IN
    ('discovered','certified','stale','invalidated','closed')),
  legs_json TEXT NOT NULL,
  UNIQUE(group_id, revision)
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_group_revisions_current
  ON neg_risk_group_revisions(group_id, revision DESC);

CREATE TABLE IF NOT EXISTS neg_risk_group_quote_batches (
  id TEXT PRIMARY KEY,
  group_id TEXT NOT NULL,
  group_revision INTEGER NOT NULL,
  membership_hash TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL,
  quoted_at_ms INTEGER NOT NULL,
  status TEXT NOT NULL CHECK(status IN ('complete','failed','superseded')),
  failure_reason TEXT,
  legs_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_group_quote_batches_current
  ON neg_risk_group_quote_batches(
    group_id, membership_hash, status, quoted_at_ms DESC
  );

-- Opportunity-first Candidate Watcher terminal facts. Each run writes exactly
-- one row, including the controller decision that determines its next visit.
CREATE TABLE IF NOT EXISTS neg_risk_candidate_watch_facts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  group_id TEXT NOT NULL,
  membership_hash TEXT,
  quote_batch_id TEXT,
  observed_at_ms INTEGER NOT NULL,
  last_result TEXT NOT NULL CHECK(last_result IN
    ('watching','no-edge','unavailable')),
  reason TEXT,
  bundle_cost REAL,
  gross_edge_bps REAL,
  max_bundle_size REAL,
  priority_class TEXT NOT NULL CHECK(priority_class IN
    ('high','normal','explore')),
  consecutive_failures INTEGER NOT NULL CHECK(consecutive_failures >= 0),
  effective_interval_s REAL NOT NULL CHECK(effective_interval_s > 0),
  schedule_reason TEXT NOT NULL,
  next_due_at_ms INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_candidate_watch_due
  ON neg_risk_candidate_watch_facts(group_id, id DESC, next_due_at_ms);

-- Cryptographically bound receipt emitted only by publish_candidate_success()
-- in the same transaction as its complete quote batch and terminal fact.
CREATE TABLE IF NOT EXISTS neg_risk_candidate_success_receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  transaction_id TEXT NOT NULL UNIQUE,
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  quote_batch_id TEXT NOT NULL UNIQUE,
  group_revision_row_id INTEGER NOT NULL,
  quote_batch_row_id INTEGER NOT NULL,
  candidate_fact_row_id INTEGER NOT NULL UNIQUE,
  observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms >= 0),
  receipt_hash TEXT NOT NULL,
  UNIQUE(group_revision_row_id,quote_batch_row_id,candidate_fact_row_id)
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_candidate_success_receipts_group
  ON neg_risk_candidate_success_receipts(group_id,id DESC);

-- Rolling, fail-closed authority checkpoint.  The checkpoint binds the
-- retained per-group seed rows after an atomically verified history prefix is
-- compacted, so Candidate validation can replay a bounded live suffix.
CREATE TABLE IF NOT EXISTS neg_risk_candidate_authority_checkpoints (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  domain TEXT NOT NULL,
  version INTEGER NOT NULL,
  generation INTEGER NOT NULL CHECK(generation > 0),
  through_group_revision_id INTEGER NOT NULL CHECK(through_group_revision_id >= 0),
  through_quote_rowid INTEGER NOT NULL CHECK(through_quote_rowid >= 0),
  through_fact_id INTEGER NOT NULL CHECK(through_fact_id >= 0),
  through_receipt_id INTEGER NOT NULL CHECK(through_receipt_id >= 0),
  compacted_group_rows INTEGER NOT NULL CHECK(compacted_group_rows >= 0),
  compacted_quote_rows INTEGER NOT NULL CHECK(compacted_quote_rows >= 0),
  compacted_fact_rows INTEGER NOT NULL CHECK(compacted_fact_rows >= 0),
  compacted_receipt_rows INTEGER NOT NULL CHECK(compacted_receipt_rows >= 0),
  prefix_digest TEXT NOT NULL,
  seeds_json TEXT NOT NULL,
  seeds_digest TEXT NOT NULL,
  checkpoint_hash TEXT NOT NULL
);

-- Bounded Discovery publishes its cursor and every fact derived from one
-- Gamma page in one SQLite transaction.  The cursor is an opaque token.
CREATE TABLE IF NOT EXISTS neg_risk_discovery_state (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  next_cursor TEXT,
  completed INTEGER NOT NULL CHECK(completed IN (0,1)),
  last_started_at_ms INTEGER NOT NULL,
  last_finished_at_ms INTEGER NOT NULL,
  page_event_count INTEGER NOT NULL CHECK(page_event_count >= 0),
  groups_seen INTEGER NOT NULL CHECK(groups_seen >= 0),
  promoted_count INTEGER NOT NULL CHECK(promoted_count >= 0)
);

CREATE TABLE IF NOT EXISTS neg_risk_group_schedule (
  group_id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  quality TEXT NOT NULL CHECK(quality IN
    ('complete-supported','complete-unsupported','incomplete-source')),
  reason TEXT,
  gross_edge_bps TEXT NOT NULL,
  activity_rank TEXT NOT NULL,
  liquidity_rank TEXT NOT NULL,
  change_rank TEXT NOT NULL,
  age_rank TEXT NOT NULL,
  priority_score TEXT NOT NULL,
  priority_reason TEXT NOT NULL,
  priority_class TEXT NOT NULL CHECK(priority_class IN
    ('high','normal','explore')),
  liquidity_weight TEXT NOT NULL,
  first_discovered_at_ms INTEGER NOT NULL,
  last_discovered_at_ms INTEGER NOT NULL,
  last_visited_at_ms INTEGER,
  promoted_at_ms INTEGER,
  promotion_eligible_at_ms INTEGER,
  promotion_queue_deadline_at_ms INTEGER,
  candidate_start_deadline_at_ms INTEGER
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_group_schedule_priority
  ON neg_risk_group_schedule(promoted_at_ms, priority_class, group_id);

CREATE TABLE IF NOT EXISTS neg_risk_coverage_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sampled_at_ms INTEGER NOT NULL,
  group_id TEXT NOT NULL,
  source_cursor TEXT,
  liquidity_weight TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_coverage_samples_window
  ON neg_risk_coverage_samples(sampled_at_ms, group_id);

CREATE TABLE IF NOT EXISTS neg_risk_discovery_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sweep_id INTEGER NOT NULL CHECK(sweep_id >= 1),
  batch_sequence INTEGER NOT NULL CHECK(batch_sequence >= 1),
  requested_cursor TEXT,
  next_cursor TEXT,
  completed INTEGER NOT NULL CHECK(completed IN (0,1)),
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER NOT NULL,
  page_event_count INTEGER NOT NULL CHECK(page_event_count >= 0),
  groups_seen INTEGER NOT NULL CHECK(groups_seen >= 0),
  promoted_count INTEGER NOT NULL CHECK(promoted_count >= 0)
);

CREATE TABLE IF NOT EXISTS neg_risk_discovery_batch_samples (
  batch_id INTEGER NOT NULL,
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  quality TEXT NOT NULL CHECK(quality IN
    ('complete-supported','complete-unsupported','incomplete-source')),
  reason TEXT,
  liquidity_weight TEXT NOT NULL,
  promoted INTEGER NOT NULL CHECK(promoted IN (0,1)),
  PRIMARY KEY(batch_id, group_id)
);

CREATE TABLE IF NOT EXISTS neg_risk_discovery_schedule_evidence (
  batch_id INTEGER NOT NULL,
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  quality TEXT NOT NULL CHECK(quality IN
    ('complete-supported','complete-unsupported','incomplete-source')),
  reason TEXT,
  promoted INTEGER NOT NULL CHECK(promoted IN (0,1)),
  effective_at_ms INTEGER NOT NULL CHECK(effective_at_ms >= 0),
  PRIMARY KEY(batch_id, group_id)
);

CREATE TABLE IF NOT EXISTS neg_risk_discovery_authority_checkpoints (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  domain TEXT NOT NULL,
  version INTEGER NOT NULL,
  generation INTEGER NOT NULL CHECK(generation > 0),
  through_batch_id INTEGER NOT NULL CHECK(through_batch_id >= 0),
  through_sample_id INTEGER NOT NULL CHECK(through_sample_id >= 0),
  through_evidence_id INTEGER NOT NULL CHECK(through_evidence_id >= 0),
  compacted_batch_rows INTEGER NOT NULL CHECK(compacted_batch_rows >= 0),
  compacted_sample_rows INTEGER NOT NULL CHECK(compacted_sample_rows >= 0),
  compacted_evidence_rows INTEGER NOT NULL CHECK(compacted_evidence_rows >= 0),
  prefix_digest TEXT NOT NULL,
  anchor_json TEXT NOT NULL,
  anchor_digest TEXT NOT NULL,
  checkpoint_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_discovery_load_state (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  degraded_streak INTEGER NOT NULL CHECK(degraded_streak >= 0),
  last_reason TEXT,
  last_decision TEXT NOT NULL CHECK(last_decision IN
    ('fresh','yield','probe')),
  probe_every_cycles INTEGER NOT NULL CHECK(probe_every_cycles >= 2),
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_discovery_admission_state (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  effective_capacity INTEGER NOT NULL CHECK(effective_capacity >= 0),
  candidate_max_wait_ms INTEGER NOT NULL CHECK(
    candidate_max_wait_ms > 0 AND candidate_max_wait_ms <= 60000),
  selection_budget_ms INTEGER NOT NULL CHECK(selection_budget_ms > 0),
  poll_interval_ms INTEGER NOT NULL CHECK(poll_interval_ms > 0),
  group_timeout_ms INTEGER NOT NULL CHECK(group_timeout_ms > 0),
  terminal_write_budget_ms INTEGER NOT NULL CHECK(
    terminal_write_budget_ms >= 5000),
  attempt_start_write_budget_ms INTEGER NOT NULL CHECK(
    attempt_start_write_budget_ms >= 5000),
  high_burst_groups INTEGER NOT NULL CHECK(high_burst_groups > 0),
  reserved_non_high_slots INTEGER NOT NULL CHECK(reserved_non_high_slots > 0),
  effective_start_bound_ms INTEGER,
  updated_at_ms INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_candidate_attempt_starts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  promoted_at_ms INTEGER NOT NULL CHECK(promoted_at_ms >= 0),
  candidate_max_wait_ms INTEGER NOT NULL CHECK(
    candidate_max_wait_ms > 0 AND candidate_max_wait_ms <= 60000),
  started_at_ms INTEGER NOT NULL CHECK(started_at_ms >= 0),
  candidate_start_deadline_at_ms INTEGER NOT NULL CHECK(
    candidate_start_deadline_at_ms >= 0),
  deadline_breached INTEGER NOT NULL CHECK(deadline_breached IN (0,1))
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_candidate_attempt_start_group
  ON neg_risk_candidate_attempt_starts(group_id,id);

CREATE TABLE IF NOT EXISTS neg_risk_candidate_admissions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  promoted_at_ms INTEGER NOT NULL CHECK(promoted_at_ms >= 0),
  candidate_start_deadline_at_ms INTEGER NOT NULL CHECK(
    candidate_start_deadline_at_ms >= 0),
  effective_capacity INTEGER NOT NULL CHECK(effective_capacity > 0),
  candidate_max_wait_ms INTEGER NOT NULL CHECK(
    candidate_max_wait_ms > 0 AND candidate_max_wait_ms <= 60000),
  selection_budget_ms INTEGER NOT NULL CHECK(selection_budget_ms > 0),
  poll_interval_ms INTEGER NOT NULL CHECK(poll_interval_ms > 0),
  group_timeout_ms INTEGER NOT NULL CHECK(group_timeout_ms > 0),
  terminal_write_budget_ms INTEGER NOT NULL CHECK(
    terminal_write_budget_ms >= 5000),
  attempt_start_write_budget_ms INTEGER NOT NULL CHECK(
    attempt_start_write_budget_ms >= 5000),
  high_burst_groups INTEGER NOT NULL CHECK(high_burst_groups > 0),
  reserved_non_high_slots INTEGER NOT NULL CHECK(reserved_non_high_slots > 0),
  effective_start_bound_ms INTEGER NOT NULL,
  recorded_at_ms INTEGER NOT NULL CHECK(recorded_at_ms >= 0),
  UNIQUE(
    group_id,event_id,membership_hash,promoted_at_ms,
    candidate_start_deadline_at_ms)
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_candidate_admission_identity
  ON neg_risk_candidate_admissions(
    group_id,event_id,membership_hash,promoted_at_ms);

CREATE TABLE IF NOT EXISTS neg_risk_discovery_status_projection (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  domain TEXT NOT NULL,
  version INTEGER NOT NULL,
  generation INTEGER NOT NULL CHECK(generation > 0),
  raw_authority_seq INTEGER NOT NULL CHECK(raw_authority_seq >= 0),
  owner_journal_id INTEGER NOT NULL DEFAULT 0 CHECK(owner_journal_id >= 0),
  groups_json TEXT NOT NULL,
  candidate_attempt_start_count INTEGER NOT NULL CHECK(
    candidate_attempt_start_count >= 0),
  candidate_start_deadline_breach_count INTEGER NOT NULL CHECK(
    candidate_start_deadline_breach_count >= 0),
  group_count INTEGER NOT NULL DEFAULT 0,
  queue_high INTEGER NOT NULL DEFAULT 0,
  queue_normal INTEGER NOT NULL DEFAULT 0,
  queue_explore INTEGER NOT NULL DEFAULT 0,
  promotion_queue_depth INTEGER NOT NULL DEFAULT 0,
  outstanding_admitted_count INTEGER NOT NULL DEFAULT 0,
  total_liquidity_weight REAL NOT NULL DEFAULT 0,
  projection_digest TEXT NOT NULL,
  checkpoint_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_discovery_group_projection (
  group_id TEXT PRIMARY KEY,
  visit_anchor_ms INTEGER,
  payload_json TEXT NOT NULL,
  row_hash TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_discovery_projection_oldest
  ON neg_risk_discovery_group_projection(visit_anchor_ms,group_id);

CREATE TABLE IF NOT EXISTS neg_risk_discovery_status_raw_guard (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  authority_seq INTEGER NOT NULL CHECK(authority_seq >= 0),
  candidate_attempt_start_count INTEGER NOT NULL CHECK(
    candidate_attempt_start_count >= 0),
  candidate_start_deadline_breach_count INTEGER NOT NULL CHECK(
    candidate_start_deadline_breach_count >= 0)
);

CREATE TRIGGER IF NOT EXISTS trg_discovery_guard_admission_insert
AFTER INSERT ON neg_risk_candidate_admissions BEGIN
  UPDATE neg_risk_discovery_status_raw_guard
  SET authority_seq=authority_seq+1 WHERE id=1;
END;
CREATE TRIGGER IF NOT EXISTS trg_discovery_guard_admission_update
AFTER UPDATE ON neg_risk_candidate_admissions BEGIN
  UPDATE neg_risk_discovery_status_raw_guard
  SET authority_seq=authority_seq+1 WHERE id=1;
END;
CREATE TRIGGER IF NOT EXISTS trg_discovery_guard_admission_delete
AFTER DELETE ON neg_risk_candidate_admissions BEGIN
  UPDATE neg_risk_discovery_status_raw_guard
  SET authority_seq=authority_seq+1 WHERE id=1;
END;
CREATE TRIGGER IF NOT EXISTS trg_discovery_guard_attempt_insert
AFTER INSERT ON neg_risk_candidate_attempt_starts BEGIN
  UPDATE neg_risk_discovery_status_raw_guard SET
    authority_seq=authority_seq+1,
    candidate_attempt_start_count=candidate_attempt_start_count+1,
    candidate_start_deadline_breach_count=
      candidate_start_deadline_breach_count+NEW.deadline_breached
  WHERE id=1;
END;
CREATE TRIGGER IF NOT EXISTS trg_discovery_guard_attempt_update
AFTER UPDATE ON neg_risk_candidate_attempt_starts BEGIN
  UPDATE neg_risk_discovery_status_raw_guard SET
    authority_seq=authority_seq+1,
    candidate_start_deadline_breach_count=
      candidate_start_deadline_breach_count+
      NEW.deadline_breached-OLD.deadline_breached
  WHERE id=1;
END;
CREATE TRIGGER IF NOT EXISTS trg_discovery_guard_attempt_delete
AFTER DELETE ON neg_risk_candidate_attempt_starts BEGIN
  UPDATE neg_risk_discovery_status_raw_guard SET
    authority_seq=authority_seq+1,
    candidate_attempt_start_count=candidate_attempt_start_count-1,
    candidate_start_deadline_breach_count=
      candidate_start_deadline_breach_count-OLD.deadline_breached
  WHERE id=1;
END;

CREATE TABLE IF NOT EXISTS neg_risk_owner_write_context (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  writer_token TEXT NOT NULL,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL CHECK(operation IN ('INSERT','UPDATE','DELETE')),
  row_key TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_owner_mutation_journal (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  writer_token TEXT,
  table_name TEXT NOT NULL,
  operation TEXT NOT NULL CHECK(operation IN ('INSERT','UPDATE','DELETE')),
  row_key TEXT NOT NULL,
  old_json TEXT,
  new_json TEXT,
  previous_hash TEXT,
  event_hash TEXT
);

CREATE TABLE IF NOT EXISTS neg_risk_owner_mutation_guard (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  consumed_journal_id INTEGER NOT NULL CHECK(consumed_journal_id >= 0),
  consumed_hash TEXT,
  retained_base_id INTEGER NOT NULL DEFAULT 0 CHECK(retained_base_id >= 0),
  retained_base_hash TEXT,
  candidate_aggregate_hash TEXT,
  discovery_aggregate_hash TEXT,
  authority_version INTEGER NOT NULL CHECK(authority_version = 2),
  migration_state TEXT NOT NULL CHECK(migration_state IN ('building','complete'))
);

CREATE TRIGGER IF NOT EXISTS trg_owner_candidate_fact_insert
AFTER INSERT ON neg_risk_candidate_watch_facts BEGIN
  INSERT INTO neg_risk_owner_mutation_journal(
    writer_token,table_name,operation,row_key,old_json,new_json)
  VALUES(
    (SELECT writer_token FROM neg_risk_owner_write_context WHERE id=1),
    'neg_risk_candidate_watch_facts','INSERT',NEW.group_id,NULL,
    json_object(
      'id',NEW.id,'group_id',NEW.group_id,
      'membership_hash',NEW.membership_hash,
      'quote_batch_id',NEW.quote_batch_id,
      'observed_at_ms',NEW.observed_at_ms,'last_result',NEW.last_result,
      'reason',NEW.reason,'bundle_cost',NEW.bundle_cost,
      'gross_edge_bps',NEW.gross_edge_bps,
      'max_bundle_size',NEW.max_bundle_size,
      'priority_class',NEW.priority_class,
      'consecutive_failures',NEW.consecutive_failures,
      'effective_interval_s',NEW.effective_interval_s,
      'schedule_reason',NEW.schedule_reason,
      'next_due_at_ms',NEW.next_due_at_ms));
END;
CREATE TRIGGER IF NOT EXISTS trg_owner_candidate_fact_update
AFTER UPDATE ON neg_risk_candidate_watch_facts BEGIN
  INSERT INTO neg_risk_owner_mutation_journal(
    writer_token,table_name,operation,row_key,old_json,new_json)
  VALUES(
    (SELECT writer_token FROM neg_risk_owner_write_context WHERE id=1),
    'neg_risk_candidate_watch_facts','UPDATE',NEW.group_id,
    json_object(
      'id',OLD.id,'group_id',OLD.group_id,
      'membership_hash',OLD.membership_hash,
      'quote_batch_id',OLD.quote_batch_id,
      'observed_at_ms',OLD.observed_at_ms,'last_result',OLD.last_result,
      'reason',OLD.reason,'bundle_cost',OLD.bundle_cost,
      'gross_edge_bps',OLD.gross_edge_bps,
      'max_bundle_size',OLD.max_bundle_size,
      'priority_class',OLD.priority_class,
      'consecutive_failures',OLD.consecutive_failures,
      'effective_interval_s',OLD.effective_interval_s,
      'schedule_reason',OLD.schedule_reason,
      'next_due_at_ms',OLD.next_due_at_ms),
    json_object(
      'id',NEW.id,'group_id',NEW.group_id,
      'membership_hash',NEW.membership_hash,
      'quote_batch_id',NEW.quote_batch_id,
      'observed_at_ms',NEW.observed_at_ms,'last_result',NEW.last_result,
      'reason',NEW.reason,'bundle_cost',NEW.bundle_cost,
      'gross_edge_bps',NEW.gross_edge_bps,
      'max_bundle_size',NEW.max_bundle_size,
      'priority_class',NEW.priority_class,
      'consecutive_failures',NEW.consecutive_failures,
      'effective_interval_s',NEW.effective_interval_s,
      'schedule_reason',NEW.schedule_reason,
      'next_due_at_ms',NEW.next_due_at_ms));
END;
CREATE TRIGGER IF NOT EXISTS trg_owner_candidate_fact_delete
AFTER DELETE ON neg_risk_candidate_watch_facts BEGIN
  INSERT INTO neg_risk_owner_mutation_journal(
    writer_token,table_name,operation,row_key,old_json,new_json)
  VALUES(
    (SELECT writer_token FROM neg_risk_owner_write_context WHERE id=1),
    'neg_risk_candidate_watch_facts','DELETE',OLD.group_id,
    json_object(
      'id',OLD.id,'group_id',OLD.group_id,
      'membership_hash',OLD.membership_hash,
      'quote_batch_id',OLD.quote_batch_id,
      'observed_at_ms',OLD.observed_at_ms,'last_result',OLD.last_result,
      'reason',OLD.reason,'bundle_cost',OLD.bundle_cost,
      'gross_edge_bps',OLD.gross_edge_bps,
      'max_bundle_size',OLD.max_bundle_size,
      'priority_class',OLD.priority_class,
      'consecutive_failures',OLD.consecutive_failures,
      'effective_interval_s',OLD.effective_interval_s,
      'schedule_reason',OLD.schedule_reason,
      'next_due_at_ms',OLD.next_due_at_ms),NULL);
END;

CREATE TABLE IF NOT EXISTS neg_risk_candidate_current_authority (
  group_id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  group_revision INTEGER NOT NULL CHECK(group_revision > 0),
  quote_batch_id TEXT,
  fact_id INTEGER NOT NULL CHECK(fact_id > 0),
  last_result TEXT NOT NULL CHECK(last_result IN
    ('watching','no-edge','unavailable')),
  opportunity INTEGER NOT NULL CHECK(opportunity IN (0,1)),
  legs_json TEXT,
  canonical_json TEXT NOT NULL,
  row_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_candidate_current_aggregate (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  current_group_count INTEGER NOT NULL CHECK(current_group_count >= 0),
  opportunity_count INTEGER NOT NULL CHECK(opportunity_count >= 0),
  aggregate_digest TEXT NOT NULL
);

-- Full Reconciliation is a checkpointed calibration window. Page receipts,
-- staging samples, cursor advancement, completion and final diff publication
-- are durable facts; incomplete windows never touch online group authority.
CREATE TABLE IF NOT EXISTS neg_risk_reconciliation_windows (
  id TEXT PRIMARY KEY,
  status TEXT NOT NULL CHECK(status IN ('open','complete','applied')),
  failure_reason TEXT,
  next_cursor TEXT,
  started_at_ms INTEGER NOT NULL CHECK(started_at_ms >= 0),
  checkpoint_at_ms INTEGER NOT NULL CHECK(checkpoint_at_ms >= 0),
  finished_at_ms INTEGER,
  pages_completed INTEGER NOT NULL CHECK(pages_completed >= 0),
  events_seen INTEGER NOT NULL CHECK(events_seen >= 0),
  groups_staged INTEGER NOT NULL CHECK(groups_staged >= 0),
  rejected_count INTEGER NOT NULL CHECK(rejected_count >= 0),
  observations_count INTEGER NOT NULL CHECK(observations_count >= 0),
  baseline_count INTEGER NOT NULL CHECK(baseline_count >= 0),
  baseline_digest TEXT,
  added_count INTEGER,
  changed_count INTEGER,
  closed_count INTEGER,
  unchanged_count INTEGER,
  applied_rejected_count INTEGER
);

CREATE TABLE IF NOT EXISTS neg_risk_reconciliation_batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  window_id TEXT NOT NULL REFERENCES neg_risk_reconciliation_windows(id),
  batch_sequence INTEGER NOT NULL CHECK(batch_sequence >= 1),
  requested_cursor TEXT,
  next_cursor TEXT,
  completed INTEGER NOT NULL CHECK(completed IN (0,1)),
  started_at_ms INTEGER NOT NULL,
  finished_at_ms INTEGER NOT NULL,
  page_event_count INTEGER NOT NULL CHECK(page_event_count >= 0),
  groups_staged INTEGER NOT NULL CHECK(groups_staged >= 0),
  observed_count INTEGER NOT NULL DEFAULT 0 CHECK(observed_count >= 0),
  unique_count INTEGER NOT NULL DEFAULT 0 CHECK(unique_count >= 0),
  update_count INTEGER NOT NULL DEFAULT 0 CHECK(update_count >= 0),
  duplicate_count INTEGER NOT NULL DEFAULT 0 CHECK(duplicate_count >= 0),
  rejected_count INTEGER NOT NULL CHECK(rejected_count >= 0),
  UNIQUE(window_id,batch_sequence)
);

CREATE TABLE IF NOT EXISTS neg_risk_reconciliation_staging (
  window_id TEXT NOT NULL REFERENCES neg_risk_reconciliation_windows(id),
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  quality TEXT NOT NULL CHECK(quality IN
    ('complete-supported','complete-unsupported','incomplete-source')),
  reason TEXT,
  legs_json TEXT,
  observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms >= 0),
  source_cursor TEXT,
  PRIMARY KEY(window_id,group_id)
);

CREATE TABLE IF NOT EXISTS neg_risk_reconciliation_baseline (
  window_id TEXT NOT NULL REFERENCES neg_risk_reconciliation_windows(id),
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  revision INTEGER NOT NULL CHECK(revision >= 1),
  membership_hash TEXT NOT NULL,
  status TEXT NOT NULL CHECK(status = 'certified'),
  PRIMARY KEY(window_id,group_id)
);

CREATE TABLE IF NOT EXISTS neg_risk_reconciliation_batch_samples (
  batch_id INTEGER NOT NULL REFERENCES neg_risk_reconciliation_batches(id),
  group_id TEXT NOT NULL,
  event_id TEXT NOT NULL,
  membership_hash TEXT NOT NULL,
  quality TEXT NOT NULL CHECK(quality IN
    ('complete-supported','complete-unsupported','incomplete-source')),
  reason TEXT,
  legs_json TEXT,
  observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms >= 0),
  source_cursor TEXT,
  materialization TEXT NOT NULL CHECK(materialization IN
    ('unique','updated','duplicate')),
  PRIMARY KEY(batch_id,group_id)
);

CREATE TABLE IF NOT EXISTS neg_risk_reconciliation_authority_checkpoints (
  window_id TEXT PRIMARY KEY REFERENCES neg_risk_reconciliation_windows(id),
  domain TEXT NOT NULL,
  version INTEGER NOT NULL,
  generation INTEGER NOT NULL CHECK(generation > 0),
  through_batch_id INTEGER NOT NULL CHECK(through_batch_id >= 0),
  through_sequence INTEGER NOT NULL CHECK(through_sequence >= 0),
  compacted_batch_rows INTEGER NOT NULL CHECK(compacted_batch_rows >= 0),
  compacted_sample_rows INTEGER NOT NULL CHECK(compacted_sample_rows >= 0),
  prefix_digest TEXT NOT NULL,
  anchor_json TEXT NOT NULL,
  anchor_digest TEXT NOT NULL,
  checkpoint_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_reconciliation_diff_evidence (
  window_id TEXT NOT NULL REFERENCES neg_risk_reconciliation_windows(id),
  group_id TEXT NOT NULL,
  action TEXT NOT NULL CHECK(action IN
    ('added','changed','closed','unchanged','rejected')),
  baseline_event_id TEXT,
  baseline_revision INTEGER,
  baseline_membership_hash TEXT,
  staged_event_id TEXT,
  staged_membership_hash TEXT,
  staged_quality TEXT,
  result_event_id TEXT,
  result_revision INTEGER,
  result_membership_hash TEXT,
  result_status TEXT,
  PRIMARY KEY(window_id,group_id,action)
);

-- Slice E: every incident transition, load-control input/output and supervised
-- child termination is an append-only durable fact.
CREATE TABLE IF NOT EXISTS neg_risk_incident_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  incident_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence >= 1),
  scope TEXT NOT NULL,
  kind TEXT NOT NULL,
  state TEXT NOT NULL CHECK(state IN
    ('detected','classified','contained','recovering','verified','escalated')),
  occurred_at_ms INTEGER NOT NULL CHECK(occurred_at_ms >= 0),
  evidence_json TEXT NOT NULL,
  UNIQUE(incident_id,sequence)
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_incident_scope
  ON neg_risk_incident_events(scope,kind,id);

-- Operator controls are wake-up hints for already-enabled bounded producers.
-- Authentication nonces are durable so replay protection survives restarts.
CREATE TABLE IF NOT EXISTS neg_risk_operator_auth_nonces (
  nonce TEXT PRIMARY KEY,
  request_method TEXT NOT NULL CHECK(request_method = 'POST'),
  request_path TEXT NOT NULL,
  request_timestamp_s INTEGER NOT NULL CHECK(request_timestamp_s >= 0),
  body_hash TEXT NOT NULL,
  accepted_at_ms INTEGER NOT NULL CHECK(accepted_at_ms >= 0),
  auth_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_operator_queue (
  component TEXT PRIMARY KEY CHECK(component IN ('discovery','reconciliation')),
  queued INTEGER NOT NULL CHECK(queued IN (0,1)),
  queued_at_ms INTEGER,
  consumed_at_ms INTEGER,
  request_nonce TEXT,
  request_auth_hash TEXT,
  last_sequence INTEGER NOT NULL CHECK(last_sequence >= 0),
  last_receipt_hash TEXT
);

CREATE TABLE IF NOT EXISTS neg_risk_operator_queue_receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  component TEXT NOT NULL CHECK(component IN ('discovery','reconciliation')),
  sequence INTEGER NOT NULL CHECK(sequence >= 1),
  action TEXT NOT NULL CHECK(action IN
    ('queued','coalesced','consumed','cancelled')),
  occurred_at_ms INTEGER NOT NULL CHECK(occurred_at_ms >= 0),
  auth_nonce TEXT NOT NULL,
  auth_receipt_hash TEXT NOT NULL,
  previous_hash TEXT,
  receipt_hash TEXT NOT NULL,
  UNIQUE(component,sequence),
  UNIQUE(component,action,auth_nonce)
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_operator_queue_receipts_component
  ON neg_risk_operator_queue_receipts(component,id);

CREATE TABLE IF NOT EXISTS neg_risk_operator_queue_checkpoints (
  component TEXT PRIMARY KEY CHECK(component IN ('discovery','reconciliation')),
  domain TEXT NOT NULL,
  version INTEGER NOT NULL,
  through_sequence INTEGER NOT NULL CHECK(through_sequence >= 1),
  through_receipt_hash TEXT NOT NULL,
  last_occurred_at_ms INTEGER NOT NULL CHECK(last_occurred_at_ms >= 0),
  queued INTEGER NOT NULL CHECK(queued IN (0,1)),
  queued_at_ms INTEGER,
  consumed_at_ms INTEGER,
  request_nonce TEXT,
  request_auth_hash TEXT,
  checkpoint_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_http_probe_receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_id TEXT NOT NULL,
  started_at_ms INTEGER NOT NULL CHECK(started_at_ms >= 0),
  finished_at_ms INTEGER NOT NULL CHECK(finished_at_ms >= started_at_ms),
  responsive INTEGER NOT NULL CHECK(responsive IN (0,1)),
  observed_release_id TEXT,
  probe_nonce TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_resource_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms >= 0),
  sample_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS neg_risk_resource_decisions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  sample_id INTEGER NOT NULL REFERENCES neg_risk_resource_samples(id),
  decided_at_ms INTEGER NOT NULL CHECK(decided_at_ms >= 0),
  mode TEXT NOT NULL CHECK(mode IN
    ('normal','protect-hot-path','empty-candidate-exploration')),
  reason TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence >= 1),
  decision_json TEXT NOT NULL,
  UNIQUE(sequence)
);

CREATE TABLE IF NOT EXISTS neg_risk_producer_receipts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  component TEXT NOT NULL CHECK(component IN
    ('candidate','discovery','reconciliation')),
  attempt INTEGER NOT NULL CHECK(attempt >= 1),
  started_at_ms INTEGER NOT NULL CHECK(started_at_ms >= 0),
  finished_at_ms INTEGER NOT NULL CHECK(finished_at_ms >= started_at_ms),
  outcome TEXT NOT NULL CHECK(outcome IN
    ('success','nonzero','timeout','cancelled','spawn-error')),
  exit_code INTEGER,
  stdout_tail TEXT NOT NULL,
  stderr_tail TEXT NOT NULL,
  output_hash TEXT NOT NULL,
  supervisor_run_id TEXT NOT NULL,
  child_nonce TEXT NOT NULL DEFAULT '',
  auth_domain TEXT NOT NULL,
  child_auth_hash TEXT,
  UNIQUE(component,attempt)
);

CREATE TABLE IF NOT EXISTS neg_risk_producer_child_starts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  component TEXT NOT NULL CHECK(component IN
    ('candidate','discovery','reconciliation')),
  supervisor_run_id TEXT NOT NULL,
  child_nonce TEXT NOT NULL DEFAULT '',
  attempt INTEGER NOT NULL CHECK(attempt >= 1),
  started_at_ms INTEGER NOT NULL CHECK(started_at_ms >= 0),
  auth_domain TEXT NOT NULL,
  child_auth_hash TEXT,
  claimed_at_ms INTEGER,
  UNIQUE(component,supervisor_run_id,attempt),
  UNIQUE(component,attempt)
);

CREATE TABLE IF NOT EXISTS neg_risk_producer_heartbeats (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  component TEXT NOT NULL CHECK(component IN
    ('candidate','discovery','reconciliation')),
  supervisor_run_id TEXT NOT NULL,
  child_nonce TEXT NOT NULL DEFAULT '',
  attempt INTEGER NOT NULL CHECK(attempt >= 1),
  auth_domain TEXT NOT NULL,
  child_auth_hash TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence >= 1),
  observed_at_ms INTEGER NOT NULL CHECK(observed_at_ms >= 0),
  state TEXT NOT NULL CHECK(state IN ('progress','yielded','paused')),
  UNIQUE(component,supervisor_run_id,attempt,sequence)
);
CREATE INDEX IF NOT EXISTS idx_neg_risk_producer_heartbeat_component
  ON neg_risk_producer_heartbeats(component,id);

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

OWNER_JOURNAL_TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "neg_risk_group_revisions": (
        "id", "group_id", "event_id", "revision", "membership_hash",
        "started_at_ms", "observed_at_ms", "source_cursor", "status", "legs_json",
    ),
    "neg_risk_group_schedule": (
        "group_id", "event_id", "membership_hash", "quality", "reason",
        "gross_edge_bps", "activity_rank", "liquidity_rank", "change_rank",
        "age_rank", "priority_score", "priority_reason", "priority_class",
        "liquidity_weight", "first_discovered_at_ms", "last_discovered_at_ms",
        "last_visited_at_ms", "promoted_at_ms", "promotion_eligible_at_ms",
        "promotion_queue_deadline_at_ms", "candidate_start_deadline_at_ms",
    ),
    "neg_risk_group_quote_batches": (
        "id", "group_id", "group_revision", "membership_hash", "started_at_ms",
        "quoted_at_ms", "status", "failure_reason", "legs_json",
    ),
    "neg_risk_candidate_success_receipts": (
        "id", "transaction_id", "group_id", "event_id", "membership_hash",
        "quote_batch_id", "group_revision_row_id", "quote_batch_row_id",
        "candidate_fact_row_id", "observed_at_ms", "receipt_hash",
    ),
    "neg_risk_candidate_admissions": (
        "id", "group_id", "event_id", "membership_hash", "promoted_at_ms",
        "candidate_start_deadline_at_ms", "effective_capacity",
        "candidate_max_wait_ms", "selection_budget_ms", "poll_interval_ms",
        "group_timeout_ms", "terminal_write_budget_ms",
        "attempt_start_write_budget_ms", "high_burst_groups",
        "reserved_non_high_slots", "effective_start_bound_ms", "recorded_at_ms",
    ),
    "neg_risk_candidate_attempt_starts": (
        "id", "group_id", "event_id", "membership_hash", "promoted_at_ms",
        "candidate_max_wait_ms", "started_at_ms",
        "candidate_start_deadline_at_ms", "deadline_breached",
    ),
    "neg_risk_candidate_current_authority": (
        "group_id", "event_id", "membership_hash", "group_revision",
        "quote_batch_id", "fact_id", "last_result", "opportunity", "legs_json",
        "canonical_json", "row_hash",
    ),
    "neg_risk_candidate_current_aggregate": (
        "id", "current_group_count", "opportunity_count", "aggregate_digest",
    ),
    "neg_risk_discovery_status_projection": (
        "id", "domain", "version", "generation", "raw_authority_seq",
        "owner_journal_id", "groups_json", "candidate_attempt_start_count",
        "candidate_start_deadline_breach_count", "group_count", "queue_high",
        "queue_normal", "queue_explore", "promotion_queue_depth",
        "outstanding_admitted_count", "total_liquidity_weight",
        "projection_digest", "checkpoint_hash",
    ),
    "neg_risk_discovery_group_projection": (
        "group_id", "visit_anchor_ms", "payload_json", "row_hash",
    ),
}

OWNER_JOURNAL_TABLE_ROW_KEYS: dict[str, str] = {
    "neg_risk_candidate_current_aggregate": "CAST({alias}.id AS TEXT)",
    "neg_risk_discovery_status_projection": "CAST({alias}.id AS TEXT)",
}

# Complete attachment scope for authority-sensitive triggers. This includes
# all seven raw owners, four derived owners, and the three internal
# journal/guard/context tables even though the latter intentionally have no
# canonical triggers. Any trigger attached inside this scope is part of the
# exact manifest regardless of its name.
OWNER_TRIGGER_TABLES: tuple[str, ...] = (
    "neg_risk_group_revisions",
    "neg_risk_group_schedule",
    "neg_risk_group_quote_batches",
    "neg_risk_candidate_success_receipts",
    "neg_risk_candidate_admissions",
    "neg_risk_candidate_attempt_starts",
    "neg_risk_candidate_watch_facts",
    "neg_risk_candidate_current_authority",
    "neg_risk_candidate_current_aggregate",
    "neg_risk_discovery_status_projection",
    "neg_risk_discovery_group_projection",
    "neg_risk_owner_mutation_journal",
    "neg_risk_owner_mutation_guard",
    "neg_risk_owner_write_context",
)


def _owner_journal_triggers() -> tuple[str, tuple[str, ...]]:
    statements: list[str] = []
    names: list[str] = [
        "trg_owner_candidate_fact_insert",
        "trg_owner_candidate_fact_update",
        "trg_owner_candidate_fact_delete",
    ]
    for table, columns in OWNER_JOURNAL_TABLE_COLUMNS.items():
        short = table.removeprefix("neg_risk_")
        for operation, alias in (
            ("INSERT", "NEW"),
            ("UPDATE", "NEW"),
            ("DELETE", "OLD"),
        ):
            name = f"trg_owner_{short}_{operation.lower()}"
            names.append(name)
            row_key = OWNER_JOURNAL_TABLE_ROW_KEYS.get(
                table,
                "{alias}.group_id",
            ).format(alias=alias)
            old_json = (
                "NULL"
                if operation == "INSERT"
                else "json_object("
                + ",".join(f"'{column}',OLD.{column}" for column in columns)
                + ")"
            )
            new_json = (
                "NULL"
                if operation == "DELETE"
                else "json_object("
                + ",".join(f"'{column}',NEW.{column}" for column in columns)
                + ")"
            )
            statements.append(
                f"CREATE TRIGGER IF NOT EXISTS {name} AFTER {operation} ON {table} "
                "BEGIN INSERT INTO neg_risk_owner_mutation_journal("
                "writer_token,table_name,operation,row_key,old_json,new_json) VALUES("
                "(SELECT writer_token FROM neg_risk_owner_write_context WHERE id=1),"
                f"'{table}','{operation}',{row_key},{old_json},{new_json}); END;"
            )
    return "\n".join(statements), tuple(names)


_OWNER_TRIGGER_DDL, OWNER_JOURNAL_TRIGGER_NAMES = _owner_journal_triggers()
DDL += "\n" + _OWNER_TRIGGER_DDL

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
