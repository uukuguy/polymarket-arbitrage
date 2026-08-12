from pathlib import Path

MIGRATION_PATH = Path("alembic/versions/013_m1_alert_delivery_leases.py")


def test_013_adds_fenced_delivery_leases_without_replacing_outbox() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")

    assert 'revision = "013"' in text
    assert 'down_revision = "012"' in text
    for column in ("lease_owner", "lease_epoch", "lease_expires_at"):
        assert column in text
    assert '"m1_alert_outbox"' in text


def test_013_downgrade_removes_only_delivery_lease_columns() -> None:
    text = MIGRATION_PATH.read_text(encoding="utf-8")
    _upgrade, downgrade = text.split("def downgrade() -> None:", maxsplit=1)

    for column in ("lease_expires_at", "lease_epoch", "lease_owner"):
        assert f'"{column}"' in downgrade
