"""l2_book_levels + 3 OHLC views + l2_candidates.l3_promoted_at_ts (Phase 05 Plan 01)

Revision ID: 005
Revises: 004
Create Date: 2026-06-01

Phase 05 Plan 01 — schema foundation for L2 → L3 upgrade.

This migration lands three additive changes (no DROP / RENAME / RETYPE on
existing schema, per Phase 02 LEARNINGS L15 / Phase 01.1 P7 discipline):

1. **l2_book_levels** (D-04 + D-07): new append-only table storing top-10
   levels per side per asset per WS book snapshot. ~20 rows / book event.
   Surrogate id + UNIQUE(asset_id, ts, side, level) matches the
   l2_top_of_book / l2_trades style established in Alembic 003. RLS
   anon_read + BRIN(ts) follow the canonical pattern.

2. **3 OHLC views** (D-03 + D-06): non-materialized regular views over
   l2_top_of_book.mid_price, bucketed at 1m / 5m / 1h granularities.
   Each view exposes (asset_id, bucket_ts, open, high, low, close,
   sample_count) — the standard OHLC tuple expected by lightweight-charts.

   ⚠ CRITICAL CORRECTION: CONTEXT D-03 originally proposed the TimescaleDB
   bucket function, but TimescaleDB is **deprecated and unavailable on
   Supabase Postgres 17** (the current default). Using that function
   would fail at runtime with ``function <name>(unknown, timestamp with
   time zone) does not exist``. RESEARCH §State of the Art / §Pitfall 1
   revised D-03 to use Postgres core ``date_trunc`` instead. The Wave 0
   anti-regression test enforces this by lint-checking the migration
   source for the forbidden identifier (substring) and is therefore
   careful to keep that identifier out of THIS docstring — the rationale
   is preserved without re-introducing the substring it forbids.

   Source: https://supabase.com/docs/guides/database/extensions/timescaledb
   (Supabase deprecation notice). See also
   https://github.com/orgs/supabase/discussions/23365 (real-user impact).

   Bucket strategy:
     - 1m → ``date_trunc('minute', ts)``
     - 5m → ``to_timestamp(floor(EXTRACT(epoch FROM ts) / 300) * 300)``
       (date_trunc doesn't support arbitrary minute multiples; floor() is
       the canonical PG idiom)
     - 1h → ``date_trunc('hour', ts)``

   Semantics: open = first row's mid_price by ts ASC; close = last row's
   mid_price by ts DESC; high = MAX; low = MIN. Uses ``array_agg`` with
   ordering rather than ``FIRST_VALUE``/``LAST_VALUE`` window functions
   because GROUP BY aggregation is cheaper and semantics-equivalent for
   our use case. ``WHERE mid_price IS NOT NULL`` excludes incomplete
   snapshots.

3. **l2_candidates.l3_promoted_at_ts** (D-08 / Pitfall 8 Option C):
   nullable TIMESTAMPTZ column tracking when a candidate was promoted
   to the L3 active set. Dashboard /candidates page filters on this column
   to display the "L3" badge. Reusing l2_candidates avoids a new table /
   view + inherits the existing RLS and mirror write path.

RLS / GRANT discipline (RESEARCH §Architecture):
- Tables: ``ENABLE ROW LEVEL SECURITY`` + ``CREATE POLICY anon_read``
  (same pattern as 003_l2_tables.py — service_role bypasses by default).
- Views: explicit ``GRANT SELECT ... TO anon`` because views don't
  inherit base-table RLS policies the same way. Phase 02 D-19 pattern.

Downgrade order: views → l2_candidates index → l2_candidates column →
l2_book_levels table (children before parents).
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── l2_book_levels (Phase 05 D-04 + D-07) ────────────────────────────────
    op.create_table(
        "l2_book_levels",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.Text, nullable=False),
        sa.Column(
            "ts",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("side", sa.String(8), nullable=False),  # 'BUY' | 'SELL'
        sa.Column("level", sa.SmallInteger, nullable=False),  # 1..10
        sa.Column("price", sa.Numeric(10, 6), nullable=False),
        sa.Column("size", sa.Numeric(14, 4), nullable=False),
        sa.UniqueConstraint(
            "asset_id",
            "ts",
            "side",
            "level",
            name="uq_l2_book_levels_asset_ts_side_level",
        ),
    )
    op.create_index(
        "idx_l2_book_levels_asset_ts",
        "l2_book_levels",
        ["asset_id", "ts"],
    )
    # BRIN: 10× smaller than btree-only on append-only time-series.
    # Raw SQL form is the verbatim-portable idiom (see 003_l2_tables.py).
    op.execute(
        "CREATE INDEX idx_l2_book_levels_ts_brin "
        "ON l2_book_levels USING BRIN (ts);"
    )
    op.execute("ALTER TABLE l2_book_levels ENABLE ROW LEVEL SECURITY;")
    op.execute(
        "CREATE POLICY anon_read ON l2_book_levels FOR SELECT USING (true);"
    )

    # ── l2_candidates.l3_promoted_at_ts (Phase 05 D-08 surface) ──────────────
    op.add_column(
        "l2_candidates",
        sa.Column(
            "l3_promoted_at_ts",
            sa.TIMESTAMP(timezone=True),
            nullable=True,
        ),
    )
    op.create_index(
        "idx_l2_candidates_l3_promoted",
        "l2_candidates",
        ["l3_promoted_at_ts"],
    )

    # ── OHLC views (date_trunc only — see docstring Pitfall 1 rationale) ────
    # 1m bucket: date_trunc('minute', ts) is exact-minute alignment.
    op.execute(
        """
        CREATE OR REPLACE VIEW l2_ohlc_1m AS
        SELECT
            asset_id,
            date_trunc('minute', ts)                    AS bucket_ts,
            (array_agg(mid_price ORDER BY ts ASC))[1]   AS open,
            MAX(mid_price)                              AS high,
            MIN(mid_price)                              AS low,
            (array_agg(mid_price ORDER BY ts DESC))[1]  AS close,
            COUNT(*)                                    AS sample_count
        FROM l2_top_of_book
        WHERE mid_price IS NOT NULL
        GROUP BY asset_id, date_trunc('minute', ts);
        """
    )

    # 5m bucket: date_trunc doesn't support arbitrary minute multiples, so
    # we floor() epoch seconds to the nearest 300-second boundary and
    # convert back to TIMESTAMPTZ. This is the canonical PG idiom for
    # arbitrary-width time buckets without TimescaleDB.
    op.execute(
        """
        CREATE OR REPLACE VIEW l2_ohlc_5m AS
        SELECT
            asset_id,
            to_timestamp(floor(EXTRACT(epoch FROM ts) / 300) * 300)
                AT TIME ZONE 'UTC'                      AS bucket_ts,
            (array_agg(mid_price ORDER BY ts ASC))[1]   AS open,
            MAX(mid_price)                              AS high,
            MIN(mid_price)                              AS low,
            (array_agg(mid_price ORDER BY ts DESC))[1]  AS close,
            COUNT(*)                                    AS sample_count
        FROM l2_top_of_book
        WHERE mid_price IS NOT NULL
        GROUP BY asset_id,
                 to_timestamp(floor(EXTRACT(epoch FROM ts) / 300) * 300)
                     AT TIME ZONE 'UTC';
        """
    )

    # 1h bucket: date_trunc('hour', ts) is exact-hour alignment.
    op.execute(
        """
        CREATE OR REPLACE VIEW l2_ohlc_1h AS
        SELECT
            asset_id,
            date_trunc('hour', ts)                      AS bucket_ts,
            (array_agg(mid_price ORDER BY ts ASC))[1]   AS open,
            MAX(mid_price)                              AS high,
            MIN(mid_price)                              AS low,
            (array_agg(mid_price ORDER BY ts DESC))[1]  AS close,
            COUNT(*)                                    AS sample_count
        FROM l2_top_of_book
        WHERE mid_price IS NOT NULL
        GROUP BY asset_id, date_trunc('hour', ts);
        """
    )

    # Views don't inherit base-table RLS policies the same way. Explicit
    # GRANT keeps surface whitelisted (Phase 02 D-19 pattern).
    op.execute("GRANT SELECT ON l2_ohlc_1m TO anon;")
    op.execute("GRANT SELECT ON l2_ohlc_5m TO anon;")
    op.execute("GRANT SELECT ON l2_ohlc_1h TO anon;")


def downgrade() -> None:
    # Reverse order — views depend on l2_top_of_book; l2_candidates
    # column drop precedes any further work on that table; l2_book_levels
    # is the new table and is dropped last (its indexes + constraint
    # cascade with drop_table).
    op.execute("DROP VIEW IF EXISTS l2_ohlc_1h;")
    op.execute("DROP VIEW IF EXISTS l2_ohlc_5m;")
    op.execute("DROP VIEW IF EXISTS l2_ohlc_1m;")
    op.drop_index(
        "idx_l2_candidates_l3_promoted",
        table_name="l2_candidates",
    )
    op.drop_column("l2_candidates", "l3_promoted_at_ts")
    op.drop_table("l2_book_levels")
