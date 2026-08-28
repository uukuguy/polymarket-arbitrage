"""Permit the scoped runtime role to execute fenced recovery actions.

Revision ID: 032
Revises: 031

The runtime controller was originally provisioned for observe-only rollout.
Its execute path consequently failed at the first recovery-action insert even
though the reconciler and executor were otherwise complete.  This revision
grants only the table operations used by the fenced recovery transaction; it
does not grant DELETE, TRUNCATE, sequence authority, or publication writes.
"""

from __future__ import annotations

from alembic import op

revision = "032"
down_revision = "031"
branch_labels = None
depends_on = None

RUNTIME_ROLE = "m1_runtime_controller_capability"

INSERT_TABLES = (
    "m1_job_circuits",
    "m1_recovery_target_budgets",
    "m1_recovery_actions",
    "m1_job_runtime_events",
    "m1_incidents",
    "m1_incident_events",
    "m1_alert_outbox",
)
UPDATE_TABLES = (
    "m1_job_runtime_state",
    "m1_jobs",
    "m1_job_circuits",
    "m1_job_attempts",
    "m1_recovery_target_budgets",
    "m1_recovery_actions",
    "m1_incidents",
)
ADDITIONAL_SELECT_TABLES = (
    "m1_job_runtime_events",
    "m1_incidents",
    "m1_incident_events",
    "m1_alert_outbox",
)


def upgrade() -> None:
    for table in ADDITIONAL_SELECT_TABLES:
        op.execute(f"GRANT SELECT ON TABLE public.{table} TO {RUNTIME_ROLE}")
    for table in INSERT_TABLES:
        op.execute(f"GRANT INSERT ON TABLE public.{table} TO {RUNTIME_ROLE}")
    for table in UPDATE_TABLES:
        op.execute(f"GRANT UPDATE ON TABLE public.{table} TO {RUNTIME_ROLE}")


def downgrade() -> None:
    for table in reversed(UPDATE_TABLES):
        op.execute(f"REVOKE UPDATE ON TABLE public.{table} FROM {RUNTIME_ROLE}")
    for table in reversed(INSERT_TABLES):
        op.execute(f"REVOKE INSERT ON TABLE public.{table} FROM {RUNTIME_ROLE}")
    for table in reversed(ADDITIONAL_SELECT_TABLES):
        op.execute(f"REVOKE SELECT ON TABLE public.{table} FROM {RUNTIME_ROLE}")
