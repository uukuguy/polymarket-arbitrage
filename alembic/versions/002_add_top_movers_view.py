"""add top_movers_view

Revision ID: 002
Revises: 001
Create Date: 2026-05-14

Phase 02 Plan 02-08 (F-03) — Plan 03 SUMMARY promised top_movers_view but
Alembic 001 only created snapshots + markets_latest + recipe_runs. This
migration delivers the missing view so Plan 06 dashboard has the promised
data shape ready when it lands.

Definition: top markets by "informational interest" — measured here as
proximity to the 0.5 / 50% coin-flip line (abs(mid_price - 0.5) ASC means
"most uncertain"; we want most uncertain at the TOP, so we order by that
ascending — markets closer to 0.5 are at the top).

Design note — why NOT a true price-delta diff yet:
  Real "top movers" = markets whose YES price moved the most in the last
  N hours. That requires a markets_history table or repeated reads of the
  snapshots-keyed parquet files. markets_latest by definition holds only
  one snapshot (full overwrite per push). Phase 06 will replace this view
  with a real time-windowed delta either by:
    (a) introducing a markets_history table, or
    (b) materialising the previous snapshot's narrow rows into a sister
        table on every push.
  Until then, this view exists to satisfy the Plan 03 SUMMARY contract
  and to give the dashboard a stable column shape to bind against. The
  ordering criterion is dashboard-meaningful (most uncertain = highest
  attention value) — see JOURNAL 2026-05-08 §"Top movers 不是
  top-by-liquidity" lesson.

RLS: views inherit row-level security from their base table (markets_latest
already has anon_read SELECT policy from Alembic 001), so no separate
policy is needed.
"""
from __future__ import annotations

from alembic import op

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # CREATE OR REPLACE → idempotent re-run safe (no need to DROP first).
    # Limited to 50 rows — dashboard table is paginated anyway.
    op.execute(
        """
        CREATE OR REPLACE VIEW top_movers_view AS
        SELECT
            market_id,
            question,
            question_zh,
            slug,
            event_slug,
            mid_price,
            liquidity_usd,
            volume_usd,
            end_time_ms,
            snapshot_id,
            -- Distance from 0.5 — small value = uncertain market = "top mover" proxy.
            -- Phase 06 will replace with real cross-snapshot price delta.
            abs(coalesce(mid_price, 0.5) - 0.5) AS uncertainty_score
        FROM markets_latest
        WHERE mid_price IS NOT NULL
        ORDER BY uncertainty_score ASC, liquidity_usd DESC NULLS LAST
        LIMIT 50;
        """
    )


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS top_movers_view")
