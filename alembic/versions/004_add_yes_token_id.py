"""Add yes_token_id nullable column to markets_latest (Phase 04 D-07)

Revision ID: 004
Revises: 003
Create Date: 2026-05-28

Phase 04 D-07: markets_latest.yes_token_id (nullable TEXT).

Why: the L1 → Supabase mirror narrow projection (supabase_mirror.py
_NARROW_MARKET_COLUMNS) is being widened from 10 to 11 columns. The 11th
column (yes_token_id) is needed by Phase 04 Plan 02's L2 candidate-refresh
temp-DB watchlist path (`SELECT yes_token_id FROM markets WHERE slug=?`),
which treats yes_token_id as the WS subscription asset_id.

Source of yes_token_id values: normalizer.py:107 — `str(clobTokenIds[0])
if len(...) > 0 else None`. Some markets legitimately have no
`clobTokenIds[0]` (binary-resolved / incomplete), so the column is nullable
on the consumer end (candidate_refresh.py:121 already does
`if not yes_tid: continue`).

Alembic add-only discipline (Phase 02 LEARNINGS L15 / Phase 01.1 P7):
upgrade() uses ONLY op.add_column. No DROP / RENAME / ALTER TYPE in
upgrade(). downgrade() reverses with op.drop_column for replay-test
safety (testcontainer cycles) only — production never executes downgrade.

Pre-existing rows: receive NULL for yes_token_id (add-only, no data loss).
Next mirror push (orchestrator step 7.5) will overwrite with real values
via DELETE+INSERT full-overwrite semantics on markets_latest.
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "004"
down_revision = "003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "markets_latest",
        sa.Column("yes_token_id", sa.Text, nullable=True),
    )


def downgrade() -> None:
    op.drop_column("markets_latest", "yes_token_id")
