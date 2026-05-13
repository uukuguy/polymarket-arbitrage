"""initial dashboard schema

Revision ID: 001
Revises:
Create Date: 2026-05-13

Phase 02 Plan 03 — D-02 / D-19 Supabase narrow dashboard mirror.

Creates 3 narrow tables for the Polymarket L1 dashboard (Vercel reads these
via Supabase JS SDK with anon_key + RLS — service_role NEVER reaches Vercel):

- snapshots:     one row per snapshot run (metadata, status, market_count)
- markets_latest: current snapshot's markets (narrow 10-column subset)
- recipe_runs:   scan endpoint result history for dashboard timeline

RLS policies: anon SELECT on all tables (Vercel dashboard reads).
service_role WRITE bypasses RLS by default in Supabase — no explicit write policy needed.

NOTE: service_role bypasses RLS by default in Supabase Postgres. The anon_read
policies only need to grant SELECT; INSERT/UPDATE/DELETE from service_role happens
implicitly without a policy (Supabase default for service_role). Do NOT add a
service_role write policy — it would be redundant and could create confusion.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── snapshots table ───────────────────────────────────────────────────────
    # One row per snapshot run. Mirrors SQLite snapshots table (narrow columns).
    # parquet_url is set after R2 upload in orchestrator step 7.6.
    op.create_table(
        "snapshots",
        sa.Column("id", sa.Integer, primary_key=True),           # mirror SQLite id
        sa.Column("taken_at_ms", sa.BigInteger, nullable=False),
        sa.Column("finished_at_ms", sa.BigInteger, nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),          # subset | full
        sa.Column("status", sa.String(8), nullable=False),        # ok | degraded | fail
        sa.Column("market_count", sa.Integer, nullable=False),
        sa.Column("parquet_url", sa.Text),                        # R2 URL once uploaded
        sa.Column("issue_count_by_layer", sa.JSON),               # {1: 0, 2: 3, 4: 12}
    )
    op.create_index("idx_snapshots_taken_at", "snapshots", ["taken_at_ms"])

    # ── markets_latest table ──────────────────────────────────────────────────
    # ONLY the most-recent OK snapshot's markets, narrow columns.
    # Full overwrite semantics: orchestrator DELETE + INSERT on every push.
    op.create_table(
        "markets_latest",
        sa.Column("market_id", sa.Text, primary_key=True),
        sa.Column("question", sa.Text),
        sa.Column("slug", sa.Text),
        sa.Column("event_slug", sa.Text),
        sa.Column("mid_price", sa.Float),
        sa.Column("liquidity_usd", sa.Float),
        sa.Column("volume_usd", sa.Float),
        sa.Column("end_time_ms", sa.BigInteger),
        sa.Column("snapshot_id", sa.Integer, sa.ForeignKey("snapshots.id")),
        sa.Column("question_zh", sa.Text),                        # from translations cache
    )

    # ── recipe_runs table ─────────────────────────────────────────────────────
    # Scan endpoint results recorded for dashboard timeline view.
    op.create_table(
        "recipe_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("recipe_name", sa.String(64), nullable=False),
        sa.Column("triggered_by", sa.String(32)),                 # cron | dashboard | manual
        sa.Column("run_at_ms", sa.BigInteger, nullable=False),
        sa.Column("result_count", sa.Integer),
        sa.Column("snapshot_id", sa.Integer, sa.ForeignKey("snapshots.id")),
    )

    # ── RLS policies (Supabase Auth-aware) ───────────────────────────────────
    # anon role = public Vercel dashboard reads; service_role = daemon writes.
    # NOTE: service_role bypasses RLS by default in Supabase — no write policy needed.
    # These SELECT policies allow the Vercel dashboard (using anon_key) to read all rows.
    op.execute("ALTER TABLE snapshots ENABLE ROW LEVEL SECURITY;")
    op.execute("CREATE POLICY anon_read ON snapshots FOR SELECT USING (true);")
    op.execute("ALTER TABLE markets_latest ENABLE ROW LEVEL SECURITY;")
    op.execute("CREATE POLICY anon_read ON markets_latest FOR SELECT USING (true);")
    op.execute("ALTER TABLE recipe_runs ENABLE ROW LEVEL SECURITY;")
    op.execute("CREATE POLICY anon_read ON recipe_runs FOR SELECT USING (true);")


def downgrade() -> None:
    op.drop_table("recipe_runs")
    op.drop_table("markets_latest")
    op.drop_table("snapshots")
