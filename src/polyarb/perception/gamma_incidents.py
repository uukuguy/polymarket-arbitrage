"""Durable incident boundary for bounded Gamma page failures."""

from __future__ import annotations

import json

import httpx

from polyarb.clients.gamma_client import PaginationIntegrityError
from polyarb.perception.incidents import (
    Incident,
    IncidentManager,
    RecoveryEvidenceRequiredError,
)
from polyarb.perception.store import OpportunityPerceptionStore

_SCOPES = frozenset({"discovery", "reconciliation"})
_KINDS = frozenset({"gamma-timeout", "gamma-malformed", "gamma-cursor"})


def gamma_incident_kind(error: BaseException) -> str | None:
    if isinstance(error, httpx.TimeoutException):
        return "gamma-timeout"
    if isinstance(error, json.JSONDecodeError):
        return "gamma-malformed"
    if isinstance(error, PaginationIntegrityError):
        return "gamma-cursor" if "cursor" in str(error).lower() else "gamma-malformed"
    if isinstance(error, ValueError) and str(error) in {
        "discovery-page-cursor-mismatch",
        "reconciliation-page-cursor-mismatch",
    }:
        return "gamma-cursor"
    return None


class GammaBatchIncidents:
    def __init__(
        self,
        store: OpportunityPerceptionStore,
        *,
        scope: str,
    ) -> None:
        if scope not in _SCOPES:
            raise ValueError("invalid-gamma-incident-scope")
        self._store = store
        self._scope = scope
        self._manager = IncidentManager(store)

    def record_failure(self, error: BaseException) -> Incident | None:
        kind = gamma_incident_kind(error)
        if kind is None:
            return None
        incident = self._manager.detect(
            self._scope,
            kind,
            {
                "action": "retry-next-gamma-page",
                "error_type": type(error).__name__,
            },
        )
        if incident.state == "detected":
            incident = self._manager.transition(
                incident.id,
                "classified",
                {
                    "action": "classify-gamma-page-failure",
                    "class": kind.removeprefix("gamma-"),
                },
            )
        if incident.state == "classified":
            incident = self._manager.transition(
                incident.id,
                "contained",
                {"action": "preserve-checkpoint"},
            )
        if incident.state in {"contained", "escalated"}:
            incident = self._manager.transition(
                incident.id,
                "recovering",
                {
                    "action": "retry-next-gamma-page",
                    **self._recovery_anchor(),
                },
            )
        return incident

    def verify_reconciliation(self, window_id: str) -> None:
        if self._scope != "reconciliation":
            raise ValueError("gamma-incident-scope-mismatch")
        self._verify({"window_id": window_id})

    def verify_discovery(self, batch_id: int) -> None:
        if self._scope != "discovery":
            raise ValueError("gamma-incident-scope-mismatch")
        self._verify({"batch_id": batch_id})

    def _verify(self, pointer: dict[str, object]) -> None:
        for incident in self._manager.open_incidents():
            if (
                incident.scope != self._scope
                or incident.kind not in _KINDS
                or incident.state != "recovering"
            ):
                continue
            try:
                self._manager.transition(incident.id, "verified", pointer)
            except RecoveryEvidenceRequiredError:
                continue

    def _recovery_anchor(self) -> dict[str, int]:
        if self._scope != "reconciliation":
            return {}
        window = self._store.current_reconciliation()
        return {"pages_completed": 0 if window is None else window.pages_completed}


__all__ = [
    "GammaBatchIncidents",
    "gamma_incident_kind",
]
