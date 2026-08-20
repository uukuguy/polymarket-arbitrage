"""Represent incomplete generation certification as durable waiting, not retry.

Revision ID: 018
Revises: 017
"""

from alembic import op

revision = "018"
down_revision = "017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("ck_m1_jobs_state", "m1_jobs", type_="check")
    op.create_check_constraint(
        "ck_m1_jobs_state",
        "m1_jobs",
        "state IN ('runnable', 'leased', 'retryable', 'waiting', 'checkpointed', 'succeeded', 'quarantined')",
    )
    op.drop_constraint("ck_m1_job_attempts_state", "m1_job_attempts", type_="check")
    op.create_check_constraint(
        "ck_m1_job_attempts_state",
        "m1_job_attempts",
        "state IN ('running', 'checkpointed', 'succeeded', 'retryable', 'waiting', 'quarantined')",
    )


def downgrade() -> None:
    op.drop_constraint("ck_m1_job_attempts_state", "m1_job_attempts", type_="check")
    op.create_check_constraint(
        "ck_m1_job_attempts_state",
        "m1_job_attempts",
        "state IN ('running', 'checkpointed', 'succeeded', 'retryable', 'quarantined')",
    )
    op.drop_constraint("ck_m1_jobs_state", "m1_jobs", type_="check")
    op.create_check_constraint(
        "ck_m1_jobs_state",
        "m1_jobs",
        "state IN ('runnable', 'leased', 'retryable', 'checkpointed', 'succeeded', 'quarantined')",
    )
