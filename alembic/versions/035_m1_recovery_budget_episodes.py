"""Scope recovery budgets to immutable failure episodes.

Revision ID: 035
Revises: 034

The original key made one budget permanent for a target. A repaired
executable or a new circuit failure fingerprint therefore inherited an old
incident's exhausted actions. Existing rows remain immutable history under
the explicit ``legacy`` episode; new rows name their exact attempt or circuit
failure identity.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "035"
down_revision = "034"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_index(
        "m1_recovery_target_budgets_cooldown",
        table_name="m1_recovery_target_budgets",
    )
    op.add_column(
        "m1_recovery_target_budgets",
        sa.Column("episode_key", sa.Text(), nullable=False, server_default="legacy"),
    )
    op.drop_constraint(
        "pk_m1_recovery_target_budgets",
        "m1_recovery_target_budgets",
        type_="primary",
    )
    op.create_primary_key(
        "pk_m1_recovery_target_budgets",
        "m1_recovery_target_budgets",
        ["controller_id", "target_type", "target_id", "episode_key"],
    )
    op.create_check_constraint(
        "ck_m1_recovery_target_budgets_episode_key",
        "m1_recovery_target_budgets",
        "octet_length(episode_key) BETWEEN 1 AND 160",
    )
    op.create_index(
        "m1_recovery_target_budgets_cooldown",
        "m1_recovery_target_budgets",
        [
            "controller_id",
            "target_type",
            "target_id",
            "episode_key",
            "last_next_allowed_at",
        ],
    )


def downgrade() -> None:
    """Collapse episodes conservatively for the target-only revision.

    Revision 034 cannot represent multiple episodes. Preserve the most
    restrictive remaining count and latest job cooldown, never refilling a
    target merely because executable history was collapsed.
    """
    op.drop_index(
        "m1_recovery_target_budgets_cooldown",
        table_name="m1_recovery_target_budgets",
    )
    op.drop_constraint(
        "ck_m1_recovery_target_budgets_episode_key",
        "m1_recovery_target_budgets",
        type_="check",
    )
    op.drop_constraint(
        "pk_m1_recovery_target_budgets",
        "m1_recovery_target_budgets",
        type_="primary",
    )
    op.execute(
        """
        CREATE TEMPORARY TABLE m1_recovery_budget_rollback
        ON COMMIT DROP AS
        SELECT controller_id,
               target_type,
               target_id,
               MAX(max_actions) AS max_actions,
               MIN(remaining_actions) AS remaining_actions,
               CASE
                   WHEN target_type = 'circuit' THEN NULL::timestamptz
                   ELSE MAX(last_next_allowed_at)
               END AS last_next_allowed_at,
               MIN(created_at) AS created_at,
               MAX(updated_at) AS updated_at
        FROM m1_recovery_target_budgets
        GROUP BY controller_id, target_type, target_id
        """
    )
    op.execute("DELETE FROM m1_recovery_target_budgets")
    op.execute(
        """
        INSERT INTO m1_recovery_target_budgets (
            controller_id, target_type, target_id, episode_key, max_actions,
            remaining_actions, last_next_allowed_at, created_at, updated_at
        )
        SELECT controller_id, target_type, target_id, 'legacy', max_actions,
               remaining_actions, last_next_allowed_at, created_at, updated_at
        FROM m1_recovery_budget_rollback
        """
    )
    op.create_primary_key(
        "pk_m1_recovery_target_budgets",
        "m1_recovery_target_budgets",
        ["controller_id", "target_type", "target_id"],
    )
    op.drop_column("m1_recovery_target_budgets", "episode_key")
    op.create_index(
        "m1_recovery_target_budgets_cooldown",
        "m1_recovery_target_budgets",
        ["controller_id", "target_type", "target_id", "last_next_allowed_at"],
    )
