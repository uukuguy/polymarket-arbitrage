"""Restore the legacy runtime-event-writer's exact outbox authority.

Revision ID: 037
Revises: 036

The independent watchdog's private writer owns the incident transition and its
two alert-outbox rows as one transaction.  Production proved that the legacy
login retained its incident/event grants but lacked the outbox SELECT/INSERT
pair required by ``INSERT .. ON CONFLICT``.  Keep the repair conditional so a
fresh installation without that legacy writer login remains migratable.
"""

from __future__ import annotations

from alembic import op

revision = "037"
down_revision = "036"
branch_labels = None
depends_on = None

WRITER_ROLE = "m1_runtime_event_writer"


def upgrade() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{WRITER_ROLE}'
            ) THEN
                EXECUTE 'GRANT SELECT, INSERT ON TABLE public.m1_alert_outbox TO {WRITER_ROLE}';
            END IF;
        END
        $$
        """
    )


def downgrade() -> None:
    op.execute(
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{WRITER_ROLE}'
            ) THEN
                EXECUTE 'REVOKE SELECT, INSERT ON TABLE public.m1_alert_outbox FROM {WRITER_ROLE}';
            END IF;
        END
        $$
        """
    )
