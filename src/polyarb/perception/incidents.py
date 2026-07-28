"""Append-only, evidence-verified perception incident lifecycle."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from polyarb.perception.store import OpportunityPerceptionStore

IncidentState = Literal[
    "detected", "classified", "contained", "recovering", "verified", "escalated"
]

ALLOWED: dict[IncidentState, set[IncidentState]] = {
    "detected": {"classified"},
    "classified": {"contained", "escalated"},
    "contained": {"recovering", "escalated"},
    "recovering": {"verified", "contained", "escalated"},
    "verified": set(),
    "escalated": {"recovering"},
}


class InvalidIncidentTransitionError(ValueError):
    """The requested lifecycle edge is not valid from durable latest state."""


class RecoveryEvidenceRequiredError(ValueError):
    """No authentic post-recovery writer mutation proves recovery."""


@dataclass(frozen=True)
class Incident:
    id: str
    sequence: int
    scope: str
    kind: str
    state: IncidentState
    occurred_at_ms: int
    evidence: dict[str, Any]


class IncidentManager:
    def __init__(
        self,
        store: OpportunityPerceptionStore,
        *,
        clock_ms=None,
    ) -> None:
        self._store = store
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1_000))

    def detect(self, scope: str, kind: str, evidence: dict[str, Any]) -> Incident:
        if not scope or not kind or not isinstance(evidence, dict):
            raise ValueError("invalid-incident")
        now_ms = self._clock_ms()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT * FROM neg_risk_incident_events "
                "WHERE scope=? AND kind=? ORDER BY id DESC LIMIT 1",
                (scope, kind),
            ).fetchone()
            if row is not None and row["state"] != "verified":
                con.commit()
                return self._from_row(row)
            incident_id = uuid.uuid4().hex
            con.execute(
                "INSERT INTO neg_risk_incident_events("
                "incident_id,sequence,scope,kind,state,occurred_at_ms,evidence_json"
                ") VALUES(?,?,?,?,?,?,?)",
                (
                    incident_id,
                    1,
                    scope,
                    kind,
                    "detected",
                    now_ms,
                    self._json(evidence),
                ),
            )
            row = con.execute(
                "SELECT * FROM neg_risk_incident_events WHERE id=last_insert_rowid()"
            ).fetchone()
            con.commit()
            return self._from_row(row)
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

    def transition(
        self,
        incident_id: str,
        state: IncidentState,
        evidence: dict[str, Any],
    ) -> Incident:
        if state not in ALLOWED or not isinstance(evidence, dict):
            raise InvalidIncidentTransitionError("invalid-incident-transition")
        now_ms = self._clock_ms()
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT * FROM neg_risk_incident_events "
                "WHERE incident_id=? ORDER BY sequence DESC LIMIT 1",
                (incident_id,),
            ).fetchone()
            if row is None or state not in ALLOWED[row["state"]]:
                raise InvalidIncidentTransitionError("invalid-incident-transition")
            latest = self._from_row(row)
            if state == "verified":
                recovery = con.execute(
                    "SELECT * FROM neg_risk_incident_events "
                    "WHERE incident_id=? AND state='recovering' "
                    "ORDER BY sequence DESC LIMIT 1",
                    (incident_id,),
                ).fetchone()
                if recovery is None or not self._has_recovery_proof(
                    con,
                    latest,
                    recovery_started_at_ms=recovery["occurred_at_ms"],
                    verification_at_ms=now_ms,
                    recovery_evidence=json.loads(recovery["evidence_json"]),
                    verification_evidence=evidence,
                ):
                    raise RecoveryEvidenceRequiredError("post-recovery-writer-evidence-required")
            con.execute(
                "INSERT INTO neg_risk_incident_events("
                "incident_id,sequence,scope,kind,state,occurred_at_ms,evidence_json"
                ") VALUES(?,?,?,?,?,?,?)",
                (
                    incident_id,
                    latest.sequence + 1,
                    latest.scope,
                    latest.kind,
                    state,
                    now_ms,
                    self._json(evidence),
                ),
            )
            written = con.execute(
                "SELECT * FROM neg_risk_incident_events WHERE id=last_insert_rowid()"
            ).fetchone()
            con.commit()
            return self._from_row(written)
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

    def open_incidents(self) -> tuple[Incident, ...]:
        con = self._connect(read_only=True)
        try:
            rows = con.execute(
                "SELECT * FROM neg_risk_incident_events ORDER BY incident_id,sequence"
            ).fetchall()
            histories: dict[str, list[Incident]] = {}
            for row in rows:
                event = self._from_row(row)
                histories.setdefault(event.id, []).append(event)
            latest_open: list[Incident] = []
            for history in histories.values():
                first = history[0]
                recovery: Incident | None = None
                for index, event in enumerate(history):
                    if (
                        event.sequence != index + 1
                        or event.scope != first.scope
                        or event.kind != first.kind
                        or (index == 0 and event.state != "detected")
                        or (index > 0 and event.state not in ALLOWED[history[index - 1].state])
                        or (index > 0 and event.occurred_at_ms < history[index - 1].occurred_at_ms)
                    ):
                        raise ValueError("invalid-incident-history")
                    if event.state == "recovering":
                        recovery = event
                    if event.state == "verified":
                        if recovery is None or not self._has_recovery_proof(
                            con,
                            event,
                            recovery_started_at_ms=recovery.occurred_at_ms,
                            verification_at_ms=event.occurred_at_ms,
                            recovery_evidence=recovery.evidence,
                            verification_evidence=event.evidence,
                        ):
                            raise ValueError("invalid-incident-recovery-proof")
                if history[-1].state != "verified":
                    latest_open.append(history[-1])
            return tuple(sorted(latest_open, key=lambda item: (item.occurred_at_ms, item.id)))
        finally:
            con.close()

    def _has_recovery_proof(
        self,
        con: sqlite3.Connection,
        incident: Incident,
        *,
        recovery_started_at_ms: int,
        verification_at_ms: int,
        recovery_evidence: dict[str, Any],
        verification_evidence: dict[str, Any],
    ) -> bool:
        scope = incident.scope
        if scope == "candidate" or scope.startswith("candidate:"):
            group_id = verification_evidence.get("group_id")
            if (
                not isinstance(group_id, str)
                or not group_id
                or (scope.startswith("candidate:") and group_id != scope.split(":", 1)[1])
            ):
                return False
            row = con.execute(
                "SELECT q.id,q.membership_hash,q.quoted_at_ms,c.event_id,c.status "
                "FROM neg_risk_group_quote_batches q "
                "JOIN neg_risk_group_revisions c ON "
                "c.group_id=q.group_id AND c.revision=q.group_revision "
                "WHERE q.id=? AND q.group_id=? AND q.status='complete'",
                (verification_evidence.get("quote_batch_id"), group_id),
            ).fetchone()
            return bool(
                row
                and row["quoted_at_ms"] > recovery_started_at_ms
                and row["quoted_at_ms"] <= verification_at_ms
                and row["status"] == "certified"
                and row["membership_hash"] == verification_evidence.get("membership_hash")
            )
        if scope == "discovery":
            row = con.execute(
                "SELECT * FROM neg_risk_discovery_batches WHERE id=?",
                (verification_evidence.get("batch_id"),),
            ).fetchone()
            if (
                row is None
                or row["finished_at_ms"] <= recovery_started_at_ms
                or row["finished_at_ms"] > verification_at_ms
                or row["id"]
                != con.execute("SELECT MAX(id) FROM neg_risk_discovery_batches").fetchone()[0]
            ):
                return False
            try:
                self._store.discovery_status(row["finished_at_ms"])
            except (sqlite3.Error, TypeError, ValueError):
                return False
            return bool(row["completed"] or row["next_cursor"] != row["requested_cursor"])
        if scope == "reconciliation":
            row = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
                (verification_evidence.get("window_id"),),
            ).fetchone()
            try:
                validated = self._store.current_reconciliation()
            except (sqlite3.Error, TypeError, ValueError):
                return False
            return bool(
                row
                and validated is not None
                and validated.id == row["id"]
                and row["checkpoint_at_ms"] > recovery_started_at_ms
                and row["checkpoint_at_ms"] <= verification_at_ms
                and row["pages_completed"] > int(recovery_evidence.get("pages_completed", -1))
            )
        if scope == "http":
            release_id = recovery_evidence.get("release_id")
            if verification_evidence.get("release_id") != release_id:
                return False
            row = con.execute(
                "SELECT * FROM neg_risk_http_probe_receipts "
                "WHERE release_id=? AND started_at_ms>? AND responsive=1 "
                "ORDER BY id DESC LIMIT 1",
                (release_id, recovery_started_at_ms),
            ).fetchone()
            return bool(
                row
                and row["finished_at_ms"] <= verification_at_ms
                and row["finished_at_ms"] - row["started_at_ms"] <= 2_000
            )
        if scope == "resource":
            row = con.execute(
                "SELECT * FROM neg_risk_resource_decisions WHERE id=?",
                (verification_evidence.get("decision_id"),),
            ).fetchone()
            if (
                row is None
                or row["decided_at_ms"] <= recovery_started_at_ms
                or row["decided_at_ms"] > verification_at_ms
            ):
                return False
            try:
                decision = json.loads(row["decision_json"])
            except (TypeError, ValueError):
                return False
            return bool(
                decision.get("mode") == row["mode"]
                and decision.get("reason") == row["reason"]
                and decision.get("decided_at_ms") == row["decided_at_ms"]
            )
        return False

    def _connect(self, *, read_only: bool = False) -> sqlite3.Connection:
        if read_only:
            con = sqlite3.connect(f"file:{self._store.db_path}?mode=ro", uri=True, timeout=5)
        else:
            con = sqlite3.connect(self._store.db_path, timeout=5)
        con.row_factory = sqlite3.Row
        return con

    @staticmethod
    def _json(value: dict[str, Any]) -> str:
        try:
            return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as error:
            raise ValueError("invalid-incident-evidence") from error

    @staticmethod
    def _from_row(row: sqlite3.Row) -> Incident:
        return Incident(
            id=row["incident_id"],
            sequence=row["sequence"],
            scope=row["scope"],
            kind=row["kind"],
            state=row["state"],
            occurred_at_ms=row["occurred_at_ms"],
            evidence=json.loads(row["evidence_json"]),
        )


__all__ = [
    "ALLOWED",
    "Incident",
    "IncidentManager",
    "InvalidIncidentTransitionError",
    "RecoveryEvidenceRequiredError",
]
