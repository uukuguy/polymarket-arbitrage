"""Keep bounded operator samples index-backed as durable history grows.

Revision ID: 036
Revises: 035

The operator snapshot asks for the newest attempts and pending alert rows with
small LIMITs.  Their original indexes were keyed for per-job lookup and alert
delivery scheduling, so PostgreSQL still scanned and sorted the complete
history.  Build the two read-path indexes concurrently: observability repair
must not turn into a writer outage.
"""

from __future__ import annotations

from alembic import op

revision = "036"
down_revision = "035"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(
            """
            CREATE INDEX CONCURRENTLY m1_job_attempts_latest
            ON m1_job_attempts (started_at DESC, attempt_id DESC)
            """
        )
        op.execute(
            """
            CREATE INDEX CONCURRENTLY m1_alert_outbox_pending_latest
            ON m1_alert_outbox (created_at DESC, outbox_id DESC)
            WHERE state = 'pending'
            """
        )


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS m1_alert_outbox_pending_latest")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS m1_job_attempts_latest")
