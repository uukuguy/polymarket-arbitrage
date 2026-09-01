"""Contracts for bounded semantic runtime-observe projections."""

from pathlib import Path

MIGRATION = Path("alembic/versions/042_m1_bounded_runtime_observe.py")


def test_revision_042_replaces_unbounded_raw_observe_ledger_with_bounded_projection() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "042"' in text
    assert 'down_revision = "041"' in text
    for relation in (
        "m1_runtime_observe_status",
        "m1_runtime_observe_current",
        "m1_runtime_observe_transitions",
        "m1_runtime_observe_hourly",
    ):
        assert relation in text
    assert "m1_runtime_observe_apply_turn" in text
    assert "SECURITY DEFINER" in text
    assert "m1_runtime_controller_leases" in text
    assert "coverage_truncated" in text
    assert "storage_limited" in text
    assert "LIMIT 500" in text
    assert "LIMIT 5000" in text
    assert "v_transition_count < 20" in text
    assert "m1_runtime_observe_decisions" in text
    assert "DROP TABLE" in text


def test_revision_042_revokes_direct_runtime_writes_and_grants_only_turn_ingress() -> None:
    text = MIGRATION.read_text()

    assert "m1_runtime_controller_capability" in text
    assert "REVOKE ALL ON TABLE" in text
    assert "GRANT EXECUTE ON FUNCTION public.m1_runtime_observe_apply_turn" in text
