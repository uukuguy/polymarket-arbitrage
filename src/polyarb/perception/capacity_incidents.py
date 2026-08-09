"""Durable, operator-readable lifecycle for capacity pressure episodes."""

from __future__ import annotations

from typing import Any

from polyarb.perception.incidents import Incident, IncidentManager


class CapacityIncidentLifecycle:
    """Keep storage pressure open until a receipt-backed normal measurement."""

    _SCOPE = "capacity"
    _KIND = "capacity-pressure"

    def __init__(self, incidents: IncidentManager) -> None:
        self._incidents = incidents

    @staticmethod
    def _evidence(runtime: dict[str, object]) -> dict[str, Any]:
        state = runtime.get("state")
        if state not in {"pressure", "critical", "exhaustion-imminent"}:
            raise ValueError("invalid-capacity-incident-state")
        severity = "p1" if state in {"critical", "exhaustion-imminent"} else "p2"
        return {
            "state": state,
            "free_bytes": runtime.get("free_bytes"),
            "free_percent": runtime.get("free_percent"),
            "last_action": runtime.get("last_action"),
            "consecutive_failures": runtime.get("consecutive_failures"),
            "next_retry_at_ms": runtime.get("next_attempt_at_ms"),
            "failure_reason": runtime.get("last_error_kind"),
            "impact": "storage-exhaustion-risk",
            "automatic_action": "reclaim-bounded-history",
            "next_action": "inspect-capacity-receipts",
            "severity": severity,
            "reminder_interval_s": 300 if severity == "p1" else 1800,
        }

    def observe(self, runtime: dict[str, object]) -> Incident | None:
        state = runtime.get("state")
        if state == "normal":
            verified: Incident | None = None
            for incident in self._incidents.open_incidents():
                if incident.scope != self._SCOPE or incident.kind != self._KIND:
                    continue
                verified = self._incidents.transition(
                    incident.id,
                    "verified",
                    {
                        "state": "normal",
                        "last_recovery_receipt_at_ms": runtime.get(
                            "last_recovery_receipt_at_ms"
                        ),
                    },
                )
            return verified
        evidence = self._evidence(runtime)
        incident = self._incidents.detect(self._SCOPE, self._KIND, evidence)
        if incident.state == "detected":
            incident = self._incidents.transition(incident.id, "classified", evidence)
            incident = self._incidents.transition(incident.id, "contained", evidence)
            return self._incidents.transition(incident.id, "recovering", evidence)
        if incident.state == "recovering":
            incident = self._incidents.transition(incident.id, "contained", evidence)
            return self._incidents.transition(incident.id, "recovering", evidence)
        return incident


__all__ = ["CapacityIncidentLifecycle"]
