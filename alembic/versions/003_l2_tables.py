"""l2 orderbook tracking tables (Phase 03 Plan 06 — D-07)

Revision ID: 003
Revises: 002
Create Date: 2026-05-24

Phase 03 Plan 06 D-07: dashboard mirror surface for polyarb-l2.

Creates 5 narrow tables consumed by the Vercel dashboard (anon SELECT via RLS)
and written by the L2SupabaseMirror (service_role, bypasses RLS by default):

- l2_candidates    — recipe-scanner ∪ watchlist union (D-04); diff-aware via
                     included_at_ts / removed_at_ts; ranking_score for
                     cap-truncation ordering
- l2_top_of_book   — WS price_change / best_bid_ask events flatten to best
                     bid/ask + spread + depth
- l2_trades        — WS last_trade_price events; trade_hash UNIQUE enables
                     idempotent backfill (D-08 — Polymarket Data API REST
                     7-day historical seed; re-run safe)
- l2_signals       — derived alerts (e.g. spread-spike, depth-collapse); future
                     phase consumers ACK via acknowledged_at
- l2_event_cursor  — consumer offset for catch-up after process restart
                     (consumer PK, asyncpg listener writes last_snapshot_id)

Schema discipline (Phase 02 LEARNINGS L15 — add-only):
- This migration uses only `op.create_table` + `op.create_index` + raw SQL
  for BRIN indexes and RLS policies. No DROP / RENAME / RETYPE on prior
  schema. The downgrade() reverses cleanly for replay tests.
- Subsequent migrations must use `ALTER TABLE ADD COLUMN`; never DROP an
  existing column. Tests enforce no `op.drop_*` in `upgrade()`.

RLS strategy (mirror of 001):
- All 5 tables: ENABLE ROW LEVEL SECURITY + CREATE POLICY anon_read
  FOR SELECT USING (true). service_role bypasses RLS by default — no
  explicit write policy needed.

Index strategy:
- l2_candidates: btree on (asset_id, included_at_ts) for "active candidates
  for asset" lookups; btree on (recipe_name, removed_at_ts) for "currently
  active candidates by recipe".
- l2_top_of_book / l2_trades: btree on (asset_id, ts) for time-series
  scans on a specific asset; BRIN on ts alone for full-history pruning at
  ~10x smaller index footprint than btree (BRIN: block-range; great for
  append-only time-series).
- l2_signals: btree on (acknowledged_at, ts) — unacknowledged signals
  surface first.
- l2_event_cursor: consumer is the PRIMARY KEY.

BRIN note (PostgreSQL): `op.create_index(..., postgresql_using='brin')`
historically had quirks in older alembic versions; raw `op.execute("CREATE
INDEX ... USING BRIN (ts)")` is the verbatim-portable form.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── l2_candidates ─────────────────────────────────────────────────────────
    op.create_table(
        "l2_candidates",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("snapshot_id", sa.Integer, nullable=True),  # FK to snapshots.id (soft — L2 may catch up across snapshots)
        sa.Column("recipe_name", sa.String(64), nullable=False),
        sa.Column("asset_id", sa.Text, nullable=False),
        sa.Column("market_id", sa.Text, nullable=True),
        sa.Column("event_id", sa.Text, nullable=True),
        sa.Column("included_at_ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("removed_at_ts", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("ranking_score", sa.JSON, nullable=True),
        sa.Column("source", sa.String(16), nullable=False),  # 'recipe' | 'watchlist'
    )
    op.create_index(
        "idx_l2_candidates_asset_included",
        "l2_candidates",
        ["asset_id", "included_at_ts"],
    )
    op.create_index(
        "idx_l2_candidates_recipe_removed",
        "l2_candidates",
        ["recipe_name", "removed_at_ts"],
    )

    # ── l2_top_of_book ────────────────────────────────────────────────────────
    op.create_table(
        "l2_top_of_book",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.Text, nullable=False),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("best_bid", sa.Numeric(10, 6), nullable=True),
        sa.Column("best_ask", sa.Numeric(10, 6), nullable=True),
        sa.Column("spread", sa.Numeric(10, 6), nullable=True),
        sa.Column("mid_price", sa.Numeric(10, 6), nullable=True),
        sa.Column("depth_yes_usd", sa.Numeric(14, 2), nullable=True),
        sa.Column("depth_no_usd", sa.Numeric(14, 2), nullable=True),
        sa.Column("source_event", sa.String(32), nullable=True),
    )
    op.create_index(
        "idx_l2_tob_asset_ts",
        "l2_top_of_book",
        ["asset_id", "ts"],
    )
    op.execute("CREATE INDEX idx_l2_tob_ts_brin ON l2_top_of_book USING BRIN (ts);")

    # ── l2_trades ─────────────────────────────────────────────────────────────
    op.create_table(
        "l2_trades",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.Text, nullable=False),
        sa.Column("market_id", sa.Text, nullable=True),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("price", sa.Numeric(10, 6), nullable=True),
        sa.Column("size", sa.Numeric(14, 4), nullable=False),
        sa.Column("side", sa.String(8), nullable=True),
        sa.Column("taker_address", sa.Text, nullable=True),
        sa.Column("trade_hash", sa.Text, nullable=False, unique=True),  # idempotent backfill (D-08)
        sa.Column("source", sa.String(16), nullable=True),  # 'ws' | 'data-api-backfill'
    )
    op.create_index(
        "idx_l2_trades_asset_ts",
        "l2_trades",
        ["asset_id", "ts"],
    )
    op.execute("CREATE INDEX idx_l2_trades_ts_brin ON l2_trades USING BRIN (ts);")

    # ── l2_signals ────────────────────────────────────────────────────────────
    op.create_table(
        "l2_signals",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.Text, nullable=False),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("signal_type", sa.String(32), nullable=False),
        sa.Column("severity", sa.String(16), nullable=False),  # 'info' | 'warn' | 'critical'
        sa.Column("payload", sa.JSON, nullable=True),
        sa.Column("acknowledged_by", sa.Text, nullable=True),
        sa.Column("acknowledged_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index(
        "idx_l2_signals_ack_ts",
        "l2_signals",
        ["acknowledged_at", "ts"],
    )

    # ── l2_event_cursor ───────────────────────────────────────────────────────
    op.create_table(
        "l2_event_cursor",
        sa.Column("consumer", sa.Text, primary_key=True),
        sa.Column("last_snapshot_id", sa.Integer, nullable=True),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    # ── RLS: anon SELECT on all 5 tables; service_role bypasses by default ───
    for tbl in (
        "l2_candidates",
        "l2_top_of_book",
        "l2_trades",
        "l2_signals",
        "l2_event_cursor",
    ):
        op.execute(f"ALTER TABLE {tbl} ENABLE ROW LEVEL SECURITY;")
        op.execute(f"CREATE POLICY anon_read ON {tbl} FOR SELECT USING (true);")


def downgrade() -> None:
    # Reverse order — children first if any FKs were added (currently soft FKs).
    op.drop_table("l2_event_cursor")
    op.drop_table("l2_signals")
    op.drop_table("l2_trades")
    op.drop_table("l2_top_of_book")
    op.drop_table("l2_candidates")
