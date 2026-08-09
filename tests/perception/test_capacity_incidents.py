from __future__ import annotations

from polyarb.perception.incidents import IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.storage.sqlite_store import SQLiteStore


def test_capacity_pressure_stays_open_until_receipted_normal_recovery(tmp_path) -> None:
    from polyarb.perception.capacity_incidents import CapacityIncidentLifecycle

    db_path = tmp_path / "state.db"
    sqlite_store = SQLiteStore(db_path)
    sqlite_store.init_schema()
    perception_store = OpportunityPerceptionStore(db_path)
    perception_store.init_schema()
    now = [1_000]
    lifecycle = CapacityIncidentLifecycle(
        IncidentManager(perception_store, clock_ms=lambda: now[0])
    )

    pressure = sqlite_store.record_capacity_controller_measurement(
        state="pressure",
        free_bytes=15,
        free_percent=15.0,
        observed_at_ms=now[0],
    )
    opened = lifecycle.observe(pressure)
    assert opened is not None
    assert (opened.scope, opened.kind, opened.state) == (
        "capacity",
        "capacity-pressure",
        "recovering",
    )
    assert opened.evidence["automatic_action"] == "reclaim-bounded-history"

    now[0] = 1_100
    sqlite_store.record_capacity_controller_reclaim(
        action="reclaimed-snapshots",
        deleted_count=1,
        deleted_ids=[42],
        completed_at_ms=now[0],
    )
    now[0] = 2_000
    recovered = sqlite_store.record_capacity_controller_measurement(
        state="normal",
        free_bytes=25,
        free_percent=25.0,
        observed_at_ms=now[0],
    )

    verified = lifecycle.observe(recovered)
    assert verified is not None
    assert verified.state == "verified"
