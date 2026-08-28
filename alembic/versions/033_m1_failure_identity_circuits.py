"""Count only consecutive identical failures toward a job circuit.

Revision ID: 033
Revises: 032

The runtime design requires repeated identical failure identities to open a
circuit.  The original table retained only a count, so unrelated failures on
the same job accumulated into a false trip.  Persist the active identity and
backfill existing nonzero circuits from their last recorded error class.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "033"
down_revision = "032"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("m1_job_circuits", sa.Column("failure_fingerprint", sa.Text(), nullable=True))
    op.execute(
        """
        UPDATE m1_job_circuits AS circuit
        SET failure_fingerprint = 'legacy:' || COALESCE(job.last_error_class, 'unknown')
        FROM m1_jobs AS job
        WHERE job.job_key = circuit.job_key
          AND circuit.consecutive_failures > 0
        """
    )
    op.create_check_constraint(
        "m1_job_circuits_failure_identity",
        "m1_job_circuits",
        "(consecutive_failures = 0 AND failure_fingerprint IS NULL) OR "
        "(consecutive_failures > 0 AND failure_fingerprint IS NOT NULL AND "
        "octet_length(failure_fingerprint) BETWEEN 1 AND 160)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "m1_job_circuits_failure_identity",
        "m1_job_circuits",
        type_="check",
    )
    op.drop_column("m1_job_circuits", "failure_fingerprint")
