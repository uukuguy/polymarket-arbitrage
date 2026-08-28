"""Persist the exact runtime deadline policy used by every attempt.

Revision ID: 028
Revises: 027
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "028"
down_revision = "027"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Older schedulers could cancel a worker without closing its attempt row.
    # Preserve the current runtime attempt so normal lease reclaim can emit the
    # authoritative failure event, but close every superseded orphan first.
    op.execute(
        """
        UPDATE m1_job_attempts AS attempt
        SET state = 'retryable',
            finished_at = COALESCE(attempt.finished_at, clock_timestamp()),
            error_class = COALESCE(attempt.error_class, 'SupersededLeaseBackfill'),
            error_detail = COALESCE(
                attempt.error_detail,
                '{"reason_code":"job.superseded-running-attempt"}'::jsonb
            )
        WHERE attempt.state = 'running'
          AND NOT EXISTS (
              SELECT 1
              FROM m1_job_runtime_state AS runtime
              WHERE runtime.attempt_id = attempt.attempt_id
          )
        """
    )
    # If the superseded attempt was also the job's current lease, closing only
    # the attempt would leave an unreclaimable leased job: claim_job requires a
    # runtime row in order to fence and close an expired lease.  Release that
    # orphan atomically so the next claimant can create a fresh epoch.
    op.execute(
        """
        UPDATE m1_jobs AS job
        SET state = 'retryable',
            lease_owner = NULL,
            lease_expires_at = NULL,
            next_attempt_at = clock_timestamp(),
            last_error_class = COALESCE(
                job.last_error_class,
                'SupersededLeaseBackfill'
            ),
            updated_at = clock_timestamp()
        WHERE job.state = 'leased'
          AND NOT EXISTS (
              SELECT 1
              FROM m1_job_runtime_state AS runtime
              WHERE runtime.job_key = job.job_key
                AND runtime.lease_epoch = job.lease_epoch
          )
        """
    )
    op.add_column(
        "m1_job_runtime_state",
        sa.Column("policy_version", sa.Text(), nullable=False, server_default="runtime-legacy-v1"),
    )
    for name in (
        "profile_lease_seconds",
        "profile_heartbeat_seconds",
        "profile_progress_seconds",
        "profile_attempt_seconds",
    ):
        op.add_column(
            "m1_job_runtime_state",
            sa.Column(name, sa.Integer(), nullable=False, server_default="1"),
        )
    op.execute(
        """
        UPDATE m1_job_runtime_state
        SET profile_lease_seconds = GREATEST(
                3, ROUND(EXTRACT(EPOCH FROM lease_deadline_at - last_heartbeat_at))::integer
            ),
            profile_heartbeat_seconds = GREATEST(
                1, ROUND(EXTRACT(EPOCH FROM heartbeat_deadline_at - last_heartbeat_at))::integer
            ),
            profile_progress_seconds = GREATEST(
                1, ROUND(EXTRACT(EPOCH FROM progress_deadline_at - last_progress_at))::integer
            ),
            profile_attempt_seconds = GREATEST(
                1, ROUND(EXTRACT(EPOCH FROM attempt_deadline_at - started_at))::integer
            )
        """
    )
    op.create_check_constraint(
        "ck_m1_runtime_state_policy_profile",
        "m1_job_runtime_state",
        "policy_version <> '' AND profile_lease_seconds > 0 "
        "AND profile_heartbeat_seconds > 0 AND profile_progress_seconds > 0 "
        "AND profile_attempt_seconds >= profile_progress_seconds",
    )
    op.alter_column("m1_job_runtime_state", "policy_version", server_default=None)
    for name in (
        "profile_lease_seconds",
        "profile_heartbeat_seconds",
        "profile_progress_seconds",
        "profile_attempt_seconds",
    ):
        op.alter_column("m1_job_runtime_state", name, server_default=None)

    # Running reducer checkpoints are ordered by a durable per-job sequence.
    # Terminal checkpoints from older revisions deliberately remain NULL and
    # are excluded from running-resume discovery.
    op.add_column(
        "m1_checkpoint_receipts",
        sa.Column("checkpoint_sequence", sa.BigInteger(), nullable=True),
    )
    op.create_unique_constraint(
        "uq_m1_checkpoint_receipts_job_sequence",
        "m1_checkpoint_receipts",
        ["job_key", "checkpoint_sequence"],
    )
    op.create_check_constraint(
        "ck_m1_checkpoint_receipts_running_sequence",
        "m1_checkpoint_receipts",
        "checkpoint_sequence IS NULL OR checkpoint_sequence > 0",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_m1_checkpoint_receipts_running_sequence",
        "m1_checkpoint_receipts",
        type_="check",
    )
    op.drop_constraint(
        "uq_m1_checkpoint_receipts_job_sequence",
        "m1_checkpoint_receipts",
        type_="unique",
    )
    op.drop_column("m1_checkpoint_receipts", "checkpoint_sequence")
    op.drop_constraint(
        "ck_m1_runtime_state_policy_profile",
        "m1_job_runtime_state",
        type_="check",
    )
    for name in (
        "profile_attempt_seconds",
        "profile_progress_seconds",
        "profile_heartbeat_seconds",
        "profile_lease_seconds",
        "policy_version",
    ):
        op.drop_column("m1_job_runtime_state", name)
