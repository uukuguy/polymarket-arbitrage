"""Remove the duplicated recovery-budget clock from circuit targets.

Revision ID: 034
Revises: 033

``m1_job_circuits.next_probe_at`` is the sole circuit timing authority.  Older
controllers copied the elapsed interval from ``opened_at`` into the recovery
budget and then added it to the current time again, compounding a five-minute
retry into multi-hour lockouts.  Circuit budgets count actions only, so clear
their derived deadline.  Job-target cooldowns remain untouched.
"""

from __future__ import annotations

from alembic import op

revision = "034"
down_revision = "033"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE m1_recovery_target_budgets
        SET last_next_allowed_at = NULL,
            updated_at = clock_timestamp()
        WHERE target_type = 'circuit'
          AND last_next_allowed_at IS NOT NULL
        """
    )


def downgrade() -> None:
    # The removed timestamp was derived, not source truth.  Reconstructing it
    # would reintroduce the compounded-clock defect.
    pass
