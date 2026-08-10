import sqlite3

from polyarb.daemon.structure_incidents import StructureIncidentLifecycle
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.storage.sqlite_store import SQLiteStore


def test_structure_timeout_becomes_operator_visible_recovering_incident(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    store = OpportunityPerceptionStore(db_path)
    incident = StructureIncidentLifecycle(IncidentManager(store)).record_failure(
        failure_kind="snapshot-subprocess-timeout", elapsed_ms=45_198, last_stage="persist"
    )
    assert incident.scope == "structure"
    assert incident.state == "recovering"
    assert incident.evidence["severity"] == "p1"
    assert incident.evidence["next_action"] == "inspect-stage-checkpoint-and-child-budget"


def test_certified_snapshot_verifies_structure_incident_with_durable_attempt_proof(
    tmp_path,
) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    store = OpportunityPerceptionStore(db_path)
    now = [1_000]
    lifecycle = StructureIncidentLifecycle(IncidentManager(store, clock_ms=lambda: now[0]))
    lifecycle.record_failure(
        failure_kind="snapshot-subprocess-timeout", elapsed_ms=45_198, last_stage="persist"
    )
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "INSERT INTO snapshot_attempts("
            "started_at_ms,finished_at_ms,outcome,snapshot_id,failure_kind"
            ") VALUES(?,?,?,?,?)",
            (1_001, 1_002, "succeeded", 42, None),
        )
    now[0] = 1_003

    lifecycle.record_success(snapshot_id=42)

    assert IncidentManager(store, clock_ms=lambda: now[0]).open_incidents() == ()
