"""Append-only, evidence-verified perception incident lifecycle."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from polyarb.perception.store import (
    OpportunityPerceptionStore,
    candidate_success_receipt_hash,
)

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
            if now_ms < latest.occurred_at_ms:
                raise InvalidIncidentTransitionError("incident-clock-regression")
            if state == "recovering" and (
                latest.scope == "candidate"
                or latest.scope.startswith("candidate:")
            ):
                evidence = {
                    **evidence,
                    "candidate_success_receipt_row_id": con.execute(
                        "SELECT COALESCE(MAX(id),0) "
                        "FROM neg_risk_candidate_success_receipts"
                    ).fetchone()[0],
                }
            if state == "recovering" and latest.scope == "http":
                evidence = {
                    **evidence,
                    "http_probe_row_id": con.execute(
                        "SELECT COALESCE(MAX(rowid),0) "
                        "FROM neg_risk_http_probe_receipts"
                    ).fetchone()[0],
                }
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
            try:
                group = self._store.current_group(group_id)
                quote = self._store.current_quote_batch(
                    group_id,
                    now_ms=verification_at_ms,
                    max_age_ms=max(1, verification_at_ms + 1),
                )
            except (sqlite3.Error, TypeError, ValueError):
                return False
            if group is None or quote is None:
                return False
            receipt_anchor = recovery_evidence.get("candidate_success_receipt_row_id")
            if type(receipt_anchor) is not int or receipt_anchor < 0:
                return False
            receipt = con.execute(
                "SELECT * FROM neg_risk_candidate_success_receipts "
                "WHERE quote_batch_id=?",
                (quote.quote_batch_id,),
            ).fetchone()
            if receipt is None:
                return False
            quote_row = con.execute(
                "SELECT rowid,* FROM neg_risk_group_quote_batches WHERE id=?",
                (receipt["quote_batch_id"],),
            ).fetchone()
            row = con.execute(
                "SELECT * FROM neg_risk_candidate_watch_facts WHERE id=?",
                (receipt["candidate_fact_row_id"],),
            ).fetchone()
            group_row = con.execute(
                "SELECT * FROM neg_risk_group_revisions WHERE id=?",
                (receipt["group_revision_row_id"],),
            ).fetchone()
            expected_hash = candidate_success_receipt_hash(
                transaction_id=receipt["transaction_id"],
                group_id=receipt["group_id"],
                event_id=receipt["event_id"],
                membership_hash=receipt["membership_hash"],
                quote_batch_id=receipt["quote_batch_id"],
                group_revision_row_id=receipt["group_revision_row_id"],
                quote_batch_row_id=receipt["quote_batch_row_id"],
                candidate_fact_row_id=receipt["candidate_fact_row_id"],
                observed_at_ms=receipt["observed_at_ms"],
            )
            return bool(
                row
                and quote_row
                and group_row
                and group.status == "certified"
                and receipt["id"] > receipt_anchor
                and receipt["receipt_hash"] == expected_hash
                and receipt["group_id"] == group_id
                and receipt["event_id"] == group.event_id
                and receipt["membership_hash"] == group.membership_hash
                and receipt["group_revision_row_id"] == group_row["id"]
                and receipt["quote_batch_row_id"] == quote_row["rowid"]
                and group_row["group_id"] == group.group_id
                and group_row["revision"] == group.revision
                and group_row["membership_hash"] == group.membership_hash
                and quote.quote_batch_id == verification_evidence.get("quote_batch_id")
                and row["group_id"] == group_id
                and row["membership_hash"] == quote.membership_hash
                and row["quote_batch_id"] == quote.quote_batch_id
                and row["last_result"] in ("watching", "no-edge")
                and row["observed_at_ms"] == quote.quoted_at_ms
                and receipt["observed_at_ms"] == quote.quoted_at_ms
                and quote.quoted_at_ms >= recovery_started_at_ms
                and quote.quoted_at_ms <= verification_at_ms
                and group.membership_hash == quote.membership_hash
                and quote.membership_hash == verification_evidence.get("membership_hash")
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
            probe_nonce = recovery_evidence.get("probe_nonce")
            probe_anchor = recovery_evidence.get("http_probe_row_id")
            if (
                not isinstance(release_id, str)
                or not release_id
                or not isinstance(probe_nonce, str)
                or not probe_nonce
                or type(probe_anchor) is not int
                or probe_anchor < 0
                or verification_evidence.get("release_id") != release_id
                or verification_evidence.get("probe_nonce") != probe_nonce
            ):
                return False
            row = con.execute(
                "SELECT rowid AS probe_row_id,* "
                "FROM neg_risk_http_probe_receipts "
                "WHERE release_id=? AND probe_nonce=? AND responsive=1 "
                "ORDER BY id DESC LIMIT 1",
                (release_id, probe_nonce),
            ).fetchone()
            return bool(
                row
                and row["probe_row_id"] > probe_anchor
                and row["observed_release_id"] == release_id
                and row["started_at_ms"] >= recovery_started_at_ms
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
                from polyarb.perception.resource_controller import (
                    validate_resource_history,
                )

                decision = validate_resource_history(con)
            except (sqlite3.Error, TypeError, ValueError):
                return False
            return bool(
                decision is not None
                and decision.sequence == row["sequence"]
                and decision.decided_at_ms == row["decided_at_ms"]
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
        try:
            evidence = json.loads(row["evidence_json"])
            values = (
                row["incident_id"],
                row["scope"],
                row["kind"],
                row["state"],
            )
            if (
                not all(isinstance(value, str) and value for value in values)
                or row["state"] not in ALLOWED
                or type(row["sequence"]) is not int
                or row["sequence"] < 1
                or type(row["occurred_at_ms"]) is not int
                or row["occurred_at_ms"] < 0
                or not isinstance(evidence, dict)
                or not all(isinstance(key, str) for key in evidence)
            ):
                raise ValueError
            # Re-encoding rejects NaN/Infinity and non-JSON semantic values.
            json.dumps(evidence, allow_nan=False)
        except (TypeError, ValueError, KeyError, json.JSONDecodeError) as error:
            raise ValueError("invalid-incident-evidence-history") from error
        return Incident(
            id=row["incident_id"],
            sequence=row["sequence"],
            scope=row["scope"],
            kind=row["kind"],
            state=row["state"],
            occurred_at_ms=row["occurred_at_ms"],
            evidence=evidence,
        )


__all__ = [
    "ALLOWED",
    "Incident",
    "IncidentManager",
    "InvalidIncidentTransitionError",
    "RecoveryEvidenceRequiredError",
]
