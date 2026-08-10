from polyarb.daemon.structure_incidents import StructureIncidentLifecycle
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.store import OpportunityPerceptionStore


def test_structure_timeout_becomes_operator_visible_recovering_incident(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    incident = StructureIncidentLifecycle(IncidentManager(store)).record_failure(
        failure_kind="snapshot-subprocess-timeout", elapsed_ms=45_198, last_stage="persist"
    )
    assert incident.scope == "structure"
    assert incident.state == "recovering"
    assert incident.evidence["severity"] == "p1"
    assert incident.evidence["next_action"] == "inspect-stage-checkpoint-and-child-budget"
