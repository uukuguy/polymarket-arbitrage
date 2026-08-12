"""Contract tests for transactional M1 control-plane migration 009."""

from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/009_m1_transactional_control_plane.py")


def test_009_chains_from_008_and_creates_control_plane_authority() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")

    assert 'revision = "009"' in text
    assert 'down_revision = "008"' in text
    for table in (
        "m1_jobs",
        "m1_job_attempts",
        "m1_checkpoint_receipts",
        "m1_quote_batch_inputs",
        "m1_quote_batch_receipts",
        "m1_structure_generation_inputs",
        "m1_structure_range_inputs",
        "m1_generation_manifests",
        "m1_publication_pointers",
        "m1_incidents",
        "m1_incident_events",
        "m1_alert_outbox",
        "m1_alert_deliveries",
    ):
        assert f'"{table}"' in text
    for column in (
        "lease_epoch",
        "lease_expires_at",
        "idempotency_key",
        "checkpoint_cursor",
        "checkpoint_digest",
    ):
        assert column in text


def test_009_keeps_jobs_fenced_and_is_reversible() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    upgrade, downgrade = text.split("def downgrade() -> None:", maxsplit=1)

    assert "m1_jobs_state" in upgrade
    assert "m1_jobs_runnable" in upgrade
    assert "m1_jobs_lease_expiry" in upgrade
    assert "m1_alert_outbox_event_channel" in upgrade
    for table in (
        "m1_alert_deliveries",
        "m1_alert_outbox",
        "m1_incident_events",
        "m1_incidents",
        "m1_publication_pointers",
        "m1_generation_manifests",
        "m1_checkpoint_receipts",
        "m1_quote_batch_receipts",
        "m1_quote_batch_inputs",
        "m1_structure_range_inputs",
        "m1_structure_generation_inputs",
        "m1_job_attempts",
        "m1_jobs",
    ):
        assert f'op.drop_table("{table}")' in downgrade
