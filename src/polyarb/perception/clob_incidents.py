"""Group-scoped durable incidents for bounded Candidate CLOB failures."""

from __future__ import annotations

import sqlite3

from py_clob_client.exceptions import PolyApiException

from polyarb.perception.incidents import (
    Incident,
    IncidentManager,
    RecoveryEvidenceRequiredError,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.routing.neg_risk_quote_collector import (
    QuoteCollectionIntegrityError,
)

_KINDS = frozenset(
    {"clob-missing-leg", "clob-429", "clob-latency", "sqlite-busy"}
)


def clob_incident_kind(error: BaseException) -> str | None:
    if isinstance(error, QuoteCollectionIntegrityError):
        return "clob-missing-leg"
    if isinstance(error, TimeoutError):
        return "clob-latency"
    if isinstance(error, sqlite3.OperationalError):
        error_code = getattr(error, "sqlite_errorcode", None)
        base_code = error_code & 0xFF if type(error_code) is int else None
        if base_code in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED} or str(
            error
        ).lower() in {
            "database is busy",
            "database is locked",
            "database schema is locked",
            "database table is locked",
        }:
            return "sqlite-busy"
    if (
        isinstance(error, PolyApiException)
        and error.status_code == 429
    ):
        return "clob-429"
    return None


class CandidateGroupIncidents:
    def __init__(self, store: OpportunityPerceptionStore) -> None:
        self._manager = IncidentManager(store)

    def record_failure(
        self,
        group_id: str,
        error: BaseException,
    ) -> Incident | None:
        if not group_id:
            raise ValueError("candidate-group-id-required")
        kind = clob_incident_kind(error)
        if kind is None:
            return None
        incident = self._manager.detect(
            f"candidate:{group_id}",
            kind,
            {
                "action": "retry-candidate-group",
                "error_type": type(error).__name__,
                "group_id": group_id,
            },
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
        return incident

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
    "clob_incident_kind",
]
