"""Durable operator-visible lifecycle for Structure producer failures."""

from __future__ import annotations

from polyarb.perception.incidents import Incident, IncidentManager


class StructureIncidentLifecycle:
    def __init__(self, incidents: IncidentManager) -> None:
        self._incidents = incidents

    def record_failure(
        self, *, failure_kind: str, elapsed_ms: int | None, last_stage: str | None
    ) -> Incident:
        evidence = {
            "severity": "p1",
            "impact": "market-map-stale",
            "automatic_action": "retry-bounded-structure-child",
            "next_action": "inspect-stage-checkpoint-and-child-budget",
            "failure_reason": failure_kind,
            "elapsed_ms": elapsed_ms,
            "last_stage": last_stage,
        }
        incident = self._incidents.detect("structure", "structure-producer-failure", evidence)
        if incident.state == "detected":
            incident = self._incidents.transition(incident.id, "classified", evidence)
            incident = self._incidents.transition(incident.id, "contained", evidence)
        if incident.state in {"contained", "recovering"}:
            if incident.state == "recovering":
                incident = self._incidents.transition(incident.id, "contained", evidence)
            incident = self._incidents.transition(incident.id, "recovering", evidence)
        return incident

    def record_success(self, *, snapshot_id: int) -> None:
        for incident in self._incidents.open_incidents():
            if incident.scope == "structure" and incident.kind == "structure-producer-failure":
                recovery_evidence = {
                    "snapshot_id": snapshot_id,
                    "automatic_action": "certified-recovery",
                }
                if incident.state == "escalated":
                    incident = self._incidents.transition(
                        incident.id,
                        "recovering",
                        recovery_evidence,
                    )
                self._incidents.transition(
                    incident.id,
                    "verified",
                    recovery_evidence,
                )
