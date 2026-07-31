"""Group-scoped durable incidents for bounded Candidate CLOB failures."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from py_clob_client.exceptions import PolyApiException

from polyarb.perception.fault_control import normalize_fault_call_id
from polyarb.perception.incidents import (
    Incident,
    IncidentManager,
    RecoveryEvidenceRequiredError,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.routing.neg_risk_quote_collector import (
    QuoteCollectionIntegrityError,
)

_KINDS = frozenset({"clob-missing-leg", "clob-429", "clob-latency", "sqlite-busy"})


@dataclass(frozen=True, slots=True)
class QualifiedCandidateIncidentReceipt:
    incident_id: str
    detection_event_id: int
    detection_sequence: int
    scope: str
    kind: str
    fault_call_id: str


def clob_incident_kind(error: BaseException) -> str | None:
    if isinstance(error, QuoteCollectionIntegrityError):
        return "clob-missing-leg"
    if isinstance(error, TimeoutError):
        return "clob-latency"
    if isinstance(error, sqlite3.OperationalError):
        error_code = getattr(error, "sqlite_errorcode", None)
        base_code = error_code & 0xFF if type(error_code) is int else None
        if base_code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED} or str(error).lower() in {
            "database is busy",
            "database is locked",
            "database schema is locked",
            "database table is locked",
        }:
            return "sqlite-busy"
    if isinstance(error, PolyApiException) and error.status_code == 429:
        return "clob-429"
    return None


class CandidateGroupIncidents:
    def __init__(
        self,
        store: OpportunityPerceptionStore,
        *,
        clock_ms=None,
    ) -> None:
        self._manager = IncidentManager(store, clock_ms=clock_ms)

    def record_failure(
        self,
        group_id: str,
        error: BaseException,
    ) -> Incident | None:
        if not group_id:
            raise ValueError("candidate-group-id-required")
        incident, _ = self._record_failure(group_id, error, fault_call_id=None)
        return incident

    def record_qualified_failure(
        self,
        group_id: str,
        error: BaseException,
    ) -> QualifiedCandidateIncidentReceipt | None:
        raw_call_id = getattr(error, "_polyarb_fault_call_id", None)
        try:
            call_id = normalize_fault_call_id(raw_call_id)
        except (TypeError, ValueError):
            return None
        _, receipt = self._record_failure(
            group_id,
            error,
            fault_call_id=call_id,
        )
        return receipt

    def _record_failure(
        self,
        group_id: str,
        error: BaseException,
        *,
        fault_call_id: str | None,
    ) -> tuple[Incident | None, QualifiedCandidateIncidentReceipt | None]:
        if not group_id:
            raise ValueError("candidate-group-id-required")
        kind = clob_incident_kind(error)
        if kind is None:
            return None, None
        evidence = {
            "action": "retry-candidate-group",
            "error_type": type(error).__name__,
            "group_id": group_id,
        }
        if fault_call_id is not None:
            evidence["fault_call_id"] = fault_call_id
        incident = self._manager.detect(
            f"candidate:{group_id}",
            kind,
            evidence,
        )
        receipt: QualifiedCandidateIncidentReceipt | None = None
        if (
            fault_call_id is not None
            and incident.sequence == 1
            and incident.state == "detected"
            and incident.evidence.get("fault_call_id") == fault_call_id
        ):
            history = self._manager.incident_history(incident.id, limit=1)
            if history is not None and history.history_complete:
                detected = history.items[0]
                receipt = QualifiedCandidateIncidentReceipt(
                    incident_id=incident.id,
                    detection_event_id=detected.event_id,
                    detection_sequence=incident.sequence,
                    scope=incident.scope,
                    kind=kind,
                    fault_call_id=fault_call_id,
                )
        if incident.state == "detected":
            incident = self._manager.transition(
                incident.id,
                "classified",
                {
                    "action": "classify-clob-group-failure",
                    "class": kind.removeprefix("clob-"),
                    "group_id": group_id,
                },
            )
        if incident.state == "classified":
            incident = self._manager.transition(
                incident.id,
                "contained",
                {
                    "action": "isolate-candidate-group",
                    "group_id": group_id,
                },
            )
        if incident.state in {"contained", "escalated"}:
            incident = self._manager.transition(
                incident.id,
                "recovering",
                {
                    "action": "retry-candidate-group",
                    "group_id": group_id,
                },
            )
        return incident, receipt

    def validate_qualified_receipt(
        self,
        receipt: QualifiedCandidateIncidentReceipt,
    ) -> bool:
        if (
            not isinstance(receipt, QualifiedCandidateIncidentReceipt)
            or receipt.kind not in _KINDS
            or receipt.detection_sequence != 1
            or not receipt.scope.startswith("candidate:")
        ):
            return False
        history = self._manager.incident_history(receipt.incident_id, limit=100)
        if history is None or not history.history_complete:
            return False
        matches = tuple(
            item
            for item in history.items
            if (
                item.event_id == receipt.detection_event_id
                and item.incident.sequence == receipt.detection_sequence
                and item.incident.scope == receipt.scope
                and item.incident.kind == receipt.kind
                and item.incident.state == "detected"
                and item.incident.evidence.get("fault_call_id") == receipt.fault_call_id
            )
        )
        return len(matches) == 1

    def verify_success(
        self,
        *,
        group_id: str,
        membership_hash: str,
        quote_batch_id: str,
    ) -> None:
        scope = f"candidate:{group_id}"
        pointer = {
            "group_id": group_id,
            "membership_hash": membership_hash,
            "quote_batch_id": quote_batch_id,
        }
        for incident in self._manager.open_incidents():
            if (
                incident.scope != scope
                or incident.kind not in _KINDS
                or incident.state != "recovering"
            ):
                continue
            try:
                self._manager.transition(incident.id, "verified", pointer)
            except RecoveryEvidenceRequiredError:
                continue


__all__ = [
    "CandidateGroupIncidents",
    "QualifiedCandidateIncidentReceipt",
    "clob_incident_kind",
]
