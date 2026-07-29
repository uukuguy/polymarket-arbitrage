"""Durable incidents derived from authenticated host resource decisions."""

from __future__ import annotations

from collections.abc import Callable

from polyarb.perception.incidents import IncidentManager
from polyarb.perception.resource_controller import ResourceDecision
from polyarb.perception.store import OpportunityPerceptionStore

_PRESSURE_KINDS = {
    "disk-pressure": "resource-disk-pressure",
    "host-contention": "resource-contention",
}


class ResourcePressureIncidents:
    def __init__(
        self,
        store: OpportunityPerceptionStore,
        *,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        self._manager = IncidentManager(store, clock_ms=clock_ms)

    def observe(self, decision: ResourceDecision, *, decision_id: int | None) -> None:
        kind = _PRESSURE_KINDS.get(decision.reason)
        if kind is not None:
            incident = self._manager.detect(
                "resource",
                kind,
                {
                    "decision_id": decision_id,
                    "reason": decision.reason,
                    "sequence": decision.sequence,
                },
            )
            if incident.state == "detected":
                incident = self._manager.transition(
                    incident.id,
                    "classified",
                    {"class": decision.reason},
                )
            if incident.state == "classified":
                incident = self._manager.transition(
                    incident.id,
                    "contained",
                    {"policy": "protect-hot-path"},
                )
            if incident.state in {"contained", "escalated"}:
                self._manager.transition(
                    incident.id,
                    "recovering",
                    {"action": "await-healthy-resource-decision"},
                )
            return
        if decision.mode != "normal" or not decision.health_claimed:
            return
        if type(decision_id) is not int or decision_id <= 0:
            raise ValueError("resource-decision-id-required")
        for incident in self._manager.open_incidents():
            if (
                incident.scope == "resource"
                and incident.kind in _PRESSURE_KINDS.values()
                and incident.state == "recovering"
            ):
                self._manager.transition(
                    incident.id,
                    "verified",
                    {"decision_id": decision_id},
                )


__all__ = ["ResourcePressureIncidents"]
