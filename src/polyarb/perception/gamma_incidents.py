"""Durable incident boundary for bounded Gamma page failures."""

from __future__ import annotations

import json
from dataclasses import dataclass

import httpx

from polyarb.clients.gamma_client import PaginationIntegrityError
from polyarb.perception.fault_control import normalize_fault_call_id
from polyarb.perception.incidents import (
    Incident,
    IncidentManager,
    RecoveryEvidenceRequiredError,
)
from polyarb.perception.store import OpportunityPerceptionStore

_SCOPES = frozenset({"discovery", "reconciliation"})
_KINDS = frozenset({"gamma-timeout", "gamma-malformed", "gamma-cursor"})


@dataclass(frozen=True, slots=True)
class QualifiedGammaIncidentReceipt:
    incident_id: str
    detection_event_id: int
    detection_sequence: int
    kind: str
    fault_call_id: str


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
        clock_ms=None,
    ) -> None:
        if scope not in _SCOPES:
            raise ValueError("invalid-gamma-incident-scope")
        self._store = store
        self._scope = scope
        self._manager = IncidentManager(store, clock_ms=clock_ms)

    def record_failure(self, error: BaseException) -> Incident | None:
        incident, _ = self._record_failure(error, fault_call_id=None)
        return incident

    def record_qualified_failure(
        self,
        error: BaseException,
    ) -> QualifiedGammaIncidentReceipt | None:
        raw_call_id = getattr(error, "_polyarb_fault_call_id", None)
        try:
            call_id = normalize_fault_call_id(raw_call_id)
        except (TypeError, ValueError):
            return None
        _, receipt = self._record_failure(error, fault_call_id=call_id)
        return receipt

    def _record_failure(
        self,
        error: BaseException,
        *,
        fault_call_id: str | None,
    ) -> tuple[Incident | None, QualifiedGammaIncidentReceipt | None]:
        kind = gamma_incident_kind(error)
        if kind is None:
            return None, None
        evidence = {
            "action": "retry-next-gamma-page",
            "error_type": type(error).__name__,
        }
        if fault_call_id is not None:
            evidence["fault_call_id"] = fault_call_id
        incident = self._manager.detect(
            self._scope,
            kind,
            evidence,
        )
        receipt: QualifiedGammaIncidentReceipt | None = None
        if (
            fault_call_id is not None
            and incident.sequence == 1
            and incident.state == "detected"
            and incident.evidence.get("fault_call_id") == fault_call_id
        ):
            history = self._manager.incident_history(incident.id, limit=1)
            if history is not None and history.history_complete:
                detected = history.items[0]
                receipt = QualifiedGammaIncidentReceipt(
                    incident_id=incident.id,
                    detection_event_id=detected.event_id,
                    detection_sequence=incident.sequence,
                    kind=kind,
                    fault_call_id=fault_call_id,
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
        return incident, receipt

    def validate_qualified_receipt(
        self,
        receipt: QualifiedGammaIncidentReceipt,
    ) -> bool:
        if (
            not isinstance(receipt, QualifiedGammaIncidentReceipt)
            or receipt.kind not in _KINDS
            or receipt.detection_sequence != 1
        ):
            return False
        history = self._manager.incident_history(
            receipt.incident_id,
            limit=100,
        )
        if history is None or not history.history_complete:
            return False
        matches = tuple(
            item
            for item in history.items
            if (
                item.event_id == receipt.detection_event_id
                and item.incident.sequence == receipt.detection_sequence
                and item.incident.scope == self._scope
                and item.incident.kind == receipt.kind
                and item.incident.state == "detected"
                and item.incident.evidence.get("fault_call_id")
                == receipt.fault_call_id
            )
        )
        return len(matches) == 1

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
    "QualifiedGammaIncidentReceipt",
    "gamma_incident_kind",
]
