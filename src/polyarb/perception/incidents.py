"""Append-only, evidence-verified perception incident lifecycle."""

from __future__ import annotations

import hashlib
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
            if row is None:
                self._store._assert_owner_journal_clean(con)
                row = con.execute(
                    "SELECT * FROM neg_risk_incident_open_authority "
                    "WHERE scope=? AND kind=? "
                    "ORDER BY occurred_at_ms DESC,incident_id DESC LIMIT 1",
                    (scope, kind),
                ).fetchone()
                if row is not None:
                    self._validate_open_authority_row(row)
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
            owner_batch = self._new_owner_batch()
            self._sync_open_authority(con, row, owner_batch)
            self._compact_events(con, owner_batch)
            self._finalize_owner_batch(con, owner_batch)
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
            if row is None:
                self._store._assert_owner_journal_clean(con)
                row = con.execute(
                    "SELECT * FROM neg_risk_incident_open_authority "
                    "WHERE incident_id=?",
                    (incident_id,),
                ).fetchone()
                if row is not None:
                    self._validate_open_authority_row(row)
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
                recovery_started_at_ms = (
                    None
                    if recovery is None
                    else int(recovery["occurred_at_ms"])
                )
                recovery_evidence = (
                    None
                    if recovery is None
                    else json.loads(str(recovery["evidence_json"]))
                )
                if recovery is None and "recovery_occurred_at_ms" in row.keys():
                    recovery_started_at_ms = row["recovery_occurred_at_ms"]
                    recovery_json = row["recovery_evidence_json"]
                    recovery_evidence = (
                        None
                        if recovery_json is None
                        else json.loads(str(recovery_json))
                    )
                if (
                    recovery_started_at_ms is None
                    or not isinstance(recovery_evidence, dict)
                    or not self._has_recovery_proof(
                    con,
                    latest,
                    recovery_started_at_ms=recovery_started_at_ms,
                    verification_at_ms=now_ms,
                    recovery_evidence=recovery_evidence,
                    verification_evidence=evidence,
                    )
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
            owner_batch = self._new_owner_batch()
            self._sync_open_authority(con, written, owner_batch)
            self._compact_events(con, owner_batch)
            self._finalize_owner_batch(con, owner_batch)
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
            self._store._assert_owner_journal_clean(con)
            self._validate_checkpoint(con)
            rows = con.execute(
                "SELECT incident_id,sequence,scope,kind,state,occurred_at_ms,"
                "evidence_json,recovery_occurred_at_ms,recovery_evidence_json,"
                "row_hash FROM neg_risk_incident_open_authority "
                "ORDER BY occurred_at_ms,incident_id"
            ).fetchall()
            aggregate = con.execute(
                "SELECT open_count,aggregate_digest FROM "
                "neg_risk_incident_open_aggregate WHERE id=1"
            ).fetchone()
            incidents = tuple(self._from_row(row) for row in rows)
            digest = 0
            authority_by_id: dict[str, sqlite3.Row] = {}
            for row in rows:
                evidence = json.loads(str(row["evidence_json"]))
                recovery_json = row["recovery_evidence_json"]
                recovery_evidence = (
                    None if recovery_json is None else json.loads(str(recovery_json))
                )
                if not isinstance(evidence, dict) or (
                    recovery_evidence is not None
                    and not isinstance(recovery_evidence, dict)
                ):
                    raise ValueError("invalid-incident-open-authority")
                payload = {
                    "evidence": evidence,
                    "incident_id": str(row["incident_id"]),
                    "kind": str(row["kind"]),
                    "occurred_at_ms": int(row["occurred_at_ms"]),
                    "recovery_evidence": recovery_evidence,
                    "recovery_occurred_at_ms": row["recovery_occurred_at_ms"],
                    "scope": str(row["scope"]),
                    "sequence": int(row["sequence"]),
                    "state": str(row["state"]),
                }
                _, expected_hash = self._row_hash(payload)
                if str(row["row_hash"]) != expected_hash:
                    raise ValueError("invalid-incident-open-authority")
                digest ^= int(expected_hash.removeprefix("sha256:"), 16)
                authority_by_id[str(row["incident_id"])] = row
            if (
                aggregate is not None
                and (
                    int(aggregate["open_count"]) != len(incidents)
                    or str(aggregate["aggregate_digest"]) != f"{digest:064x}"
                )
            ) or (
                aggregate is None and incidents
            ):
                raise ValueError("invalid-incident-open-authority")
            self._validate_retained_suffix(con, authority_by_id)
            return incidents
        finally:
            con.close()

    def _validate_checkpoint(self, con: sqlite3.Connection) -> None:
        suffix_count = int(
            con.execute("SELECT COUNT(*) FROM neg_risk_incident_events").fetchone()[0]
        )
        anchor_count = int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_incident_replay_anchors"
            ).fetchone()[0]
        )
        if suffix_count > 512 or anchor_count > 256:
            raise ValueError("invalid-incident-checkpoint")
        checkpoint = con.execute(
            "SELECT * FROM neg_risk_incident_authority_checkpoint WHERE id=1"
        ).fetchone()
        floor_count = int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_incident_scope_floors"
            ).fetchone()[0]
        )
        if checkpoint is None:
            if floor_count or anchor_count:
                raise ValueError("invalid-incident-checkpoint")
            return
        payload = {
            "compacted_event_count": int(checkpoint["compacted_event_count"]),
            "generation": int(checkpoint["generation"]),
            "prefix_hash": str(checkpoint["prefix_hash"]),
            "through_event_id": int(checkpoint["through_event_id"]),
        }
        _, expected_hash = self._row_hash(payload)
        suffix_before_floor = con.execute(
            "SELECT 1 FROM neg_risk_incident_events WHERE id<=? LIMIT 1",
            (checkpoint["through_event_id"],),
        ).fetchone()
        invalid_floor = con.execute(
            "SELECT 1 FROM neg_risk_incident_scope_floors "
            "WHERE through_event_id>? OR compacted_event_count>?"
            " OR floor_hash NOT LIKE 'sha256:%' LIMIT 1",
            (
                checkpoint["through_event_id"],
                checkpoint["compacted_event_count"],
            ),
        ).fetchone()
        if (
            str(checkpoint["checkpoint_hash"]) != expected_hash
            or not str(checkpoint["prefix_hash"]).startswith("sha256:")
            or suffix_before_floor is not None
            or invalid_floor is not None
        ):
            raise ValueError("invalid-incident-checkpoint")

    def _validate_retained_suffix(
        self,
        con: sqlite3.Connection,
        authority_by_id: dict[str, sqlite3.Row],
    ) -> None:
        rows = con.execute(
            "SELECT * FROM neg_risk_incident_events ORDER BY incident_id,sequence"
        ).fetchall()
        histories: dict[str, list[Incident]] = {}
        for row in rows:
            event = self._from_row(row)
            histories.setdefault(event.id, []).append(event)
        anchors: dict[str, Incident] = {}
        for row in con.execute(
            "SELECT * FROM neg_risk_incident_replay_anchors"
        ).fetchall():
            anchors[str(row["incident_id"])] = self._validate_replay_anchor_row(row)
        for incident_id, history in histories.items():
            first = history[0]
            if first.sequence == 1:
                if first.state != "detected" or incident_id in anchors:
                    raise ValueError("invalid-incident-history")
            else:
                anchor = anchors.pop(incident_id, None)
                if (
                    anchor is None
                    or first.sequence != anchor.sequence + 1
                    or first.scope != anchor.scope
                    or first.kind != anchor.kind
                    or first.state not in ALLOWED[anchor.state]
                    or first.occurred_at_ms < anchor.occurred_at_ms
                ):
                    raise ValueError("invalid-incident-replay-anchor")
            for previous, event in zip(history, history[1:], strict=False):
                if (
                    event.sequence != previous.sequence + 1
                    or event.scope != previous.scope
                    or event.kind != previous.kind
                    or event.state not in ALLOWED[previous.state]
                    or event.occurred_at_ms < previous.occurred_at_ms
                ):
                    raise ValueError("invalid-incident-history")
            latest = history[-1]
            authority = authority_by_id.get(incident_id)
            if latest.state == "verified":
                if authority is not None:
                    raise ValueError("invalid-incident-open-authority")
                continue
            if authority is None or any(
                (
                    authority["sequence"] != latest.sequence,
                    authority["scope"] != latest.scope,
                    authority["kind"] != latest.kind,
                    authority["state"] != latest.state,
                    authority["occurred_at_ms"] != latest.occurred_at_ms,
                    authority["evidence_json"] != self._json(latest.evidence),
                )
            ):
                raise ValueError("invalid-incident-open-authority")
        if anchors:
            raise ValueError("invalid-incident-replay-anchor")

    def _validate_open_authority_row(self, row: sqlite3.Row) -> Incident:
        incident = self._from_row(row)
        recovery_json = row["recovery_evidence_json"]
        recovery_evidence = (
            None if recovery_json is None else json.loads(str(recovery_json))
        )
        if recovery_evidence is not None and not isinstance(recovery_evidence, dict):
            raise ValueError("invalid-incident-open-authority")
        payload = {
            "evidence": incident.evidence,
            "incident_id": incident.id,
            "kind": incident.kind,
            "occurred_at_ms": incident.occurred_at_ms,
            "recovery_evidence": recovery_evidence,
            "recovery_occurred_at_ms": row["recovery_occurred_at_ms"],
            "scope": incident.scope,
            "sequence": incident.sequence,
            "state": incident.state,
        }
        _, expected_hash = self._row_hash(payload)
        if str(row["row_hash"]) != expected_hash:
            raise ValueError("invalid-incident-open-authority")
        return incident

    def _validate_replay_anchor_row(self, row: sqlite3.Row) -> Incident:
        incident = self._from_row(row)
        recovery_json = row["recovery_evidence_json"]
        recovery_evidence = (
            None if recovery_json is None else json.loads(str(recovery_json))
        )
        if recovery_evidence is not None and not isinstance(recovery_evidence, dict):
            raise ValueError("invalid-incident-replay-anchor")
        payload = {
            "evidence": incident.evidence,
            "incident_id": incident.id,
            "kind": incident.kind,
            "occurred_at_ms": incident.occurred_at_ms,
            "recovery_evidence": recovery_evidence,
            "recovery_occurred_at_ms": row["recovery_occurred_at_ms"],
            "scope": incident.scope,
            "sequence": incident.sequence,
            "state": incident.state,
        }
        _, expected_hash = self._row_hash(payload)
        if str(row["row_hash"]) != expected_hash:
            raise ValueError("invalid-incident-replay-anchor")
        return incident

    @staticmethod
    def _row_hash(payload: dict[str, Any]) -> tuple[str, str]:
        canonical = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return canonical, "sha256:" + hashlib.sha256(canonical.encode()).hexdigest()

    @staticmethod
    def _new_owner_batch() -> dict[str, Any]:
        return {"events": [], "token": None}

    def _execute_owner_mutation(
        self,
        con: sqlite3.Connection,
        batch: dict[str, Any] | None,
        *,
        table_name: str,
        operation: str,
        row_key: str,
        sql: str,
        parameters: tuple[Any, ...],
    ) -> None:
        if batch is None:
            con.execute(sql, parameters)
            return
        if batch["token"] is None:
            batch["token"] = self._store._begin_expected_owner_mutation(
                con,
                table_name=table_name,
                operation=operation,
                row_key=row_key,
            )
        con.execute(sql, parameters)
        batch["events"].append((table_name, operation, row_key))

    def _finalize_owner_batch(
        self,
        con: sqlite3.Connection,
        batch: dict[str, Any],
    ) -> None:
        if batch["token"] is None:
            return
        self._store._consume_expected_owner_events(
            con,
            writer_token=batch["token"],
            expected_events=batch["events"],
            finalize=True,
        )

    def _sync_open_authority(
        self,
        con: sqlite3.Connection,
        event: sqlite3.Row,
        owner_batch: dict[str, Any] | None,
    ) -> None:
        old = con.execute(
            "SELECT * FROM neg_risk_incident_open_authority WHERE incident_id=?",
            (event["incident_id"],),
        ).fetchone()
        aggregate = con.execute(
            "SELECT open_count,aggregate_digest FROM "
            "neg_risk_incident_open_aggregate WHERE id=1"
        ).fetchone()
        count = 0 if aggregate is None else int(aggregate["open_count"])
        digest = 0 if aggregate is None else int(str(aggregate["aggregate_digest"]), 16)
        if old is not None:
            digest ^= int(str(old["row_hash"]).removeprefix("sha256:"), 16)
            self._ensure_replay_anchor(con, event, old, owner_batch)
        if event["state"] == "verified":
            self._execute_owner_mutation(
                con,
                owner_batch,
                table_name="neg_risk_incident_open_authority",
                operation="DELETE",
                row_key=str(event["incident_id"]),
                sql=(
                    "DELETE FROM neg_risk_incident_open_authority "
                    "WHERE incident_id=?"
                ),
                parameters=(event["incident_id"],),
            )
            count -= 1
        else:
            recovery_at = (
                int(event["occurred_at_ms"])
                if event["state"] == "recovering"
                else None if old is None else old["recovery_occurred_at_ms"]
            )
            recovery_json = (
                str(event["evidence_json"])
                if event["state"] == "recovering"
                else None if old is None else old["recovery_evidence_json"]
            )
            payload = {
                "evidence": json.loads(str(event["evidence_json"])),
                "incident_id": str(event["incident_id"]),
                "kind": str(event["kind"]),
                "occurred_at_ms": int(event["occurred_at_ms"]),
                "recovery_evidence": (
                    None if recovery_json is None else json.loads(str(recovery_json))
                ),
                "recovery_occurred_at_ms": recovery_at,
                "scope": str(event["scope"]),
                "sequence": int(event["sequence"]),
                "state": str(event["state"]),
            }
            _, row_hash = self._row_hash(payload)
            self._execute_owner_mutation(
                con,
                owner_batch,
                table_name="neg_risk_incident_open_authority",
                operation="INSERT" if old is None else "UPDATE",
                row_key=str(event["incident_id"]),
                sql=(
                    "INSERT INTO neg_risk_incident_open_authority("
                    "incident_id,sequence,scope,kind,state,occurred_at_ms,"
                    "evidence_json,recovery_occurred_at_ms,"
                    "recovery_evidence_json,row_hash"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(incident_id) DO UPDATE SET "
                    "sequence=excluded.sequence,scope=excluded.scope,"
                    "kind=excluded.kind,state=excluded.state,"
                    "occurred_at_ms=excluded.occurred_at_ms,"
                    "evidence_json=excluded.evidence_json,"
                    "recovery_occurred_at_ms=excluded.recovery_occurred_at_ms,"
                    "recovery_evidence_json=excluded.recovery_evidence_json,"
                    "row_hash=excluded.row_hash"
                ),
                parameters=(
                    event["incident_id"], event["sequence"], event["scope"],
                    event["kind"], event["state"], event["occurred_at_ms"],
                    event["evidence_json"], recovery_at, recovery_json, row_hash,
                ),
            )
            digest ^= int(row_hash.removeprefix("sha256:"), 16)
            if old is None:
                count += 1
        self._execute_owner_mutation(
            con,
            owner_batch,
            table_name="neg_risk_incident_open_aggregate",
            operation="INSERT" if aggregate is None else "UPDATE",
            row_key="1",
            sql=(
                "INSERT INTO neg_risk_incident_open_aggregate("
                "id,open_count,aggregate_digest) VALUES(1,?,?) "
                "ON CONFLICT(id) DO UPDATE SET open_count=excluded.open_count,"
                "aggregate_digest=excluded.aggregate_digest"
            ),
            parameters=(count, f"{digest:064x}"),
        )

    def _ensure_replay_anchor(
        self,
        con: sqlite3.Connection,
        event: sqlite3.Row,
        predecessor: sqlite3.Row,
        owner_batch: dict[str, Any] | None,
    ) -> None:
        if int(event["sequence"]) <= 1:
            return
        retained_predecessor = con.execute(
            "SELECT 1 FROM neg_risk_incident_events "
            "WHERE incident_id=? AND sequence=?",
            (event["incident_id"], int(event["sequence"]) - 1),
        ).fetchone()
        if retained_predecessor is not None:
            return
        self._upsert_replay_anchor(
            con,
            predecessor,
            recovery_occurred_at_ms=predecessor["recovery_occurred_at_ms"],
            recovery_evidence_json=predecessor["recovery_evidence_json"],
            owner_batch=owner_batch,
        )

    def _upsert_replay_anchor(
        self,
        con: sqlite3.Connection,
        predecessor: sqlite3.Row,
        *,
        recovery_occurred_at_ms: int | None,
        recovery_evidence_json: str | None,
        owner_batch: dict[str, Any] | None,
    ) -> None:
        evidence = json.loads(str(predecessor["evidence_json"]))
        recovery_evidence = (
            None
            if recovery_evidence_json is None
            else json.loads(recovery_evidence_json)
        )
        if not isinstance(evidence, dict) or (
            recovery_evidence is not None
            and not isinstance(recovery_evidence, dict)
        ):
            raise ValueError("invalid-incident-replay-anchor")
        payload = {
            "evidence": evidence,
            "incident_id": str(predecessor["incident_id"]),
            "kind": str(predecessor["kind"]),
            "occurred_at_ms": int(predecessor["occurred_at_ms"]),
            "recovery_evidence": recovery_evidence,
            "recovery_occurred_at_ms": recovery_occurred_at_ms,
            "scope": str(predecessor["scope"]),
            "sequence": int(predecessor["sequence"]),
            "state": str(predecessor["state"]),
        }
        _, row_hash = self._row_hash(payload)
        existing = con.execute(
            "SELECT 1 FROM neg_risk_incident_replay_anchors WHERE incident_id=?",
            (predecessor["incident_id"],),
        ).fetchone()
        self._execute_owner_mutation(
            con,
            owner_batch,
            table_name="neg_risk_incident_replay_anchors",
            operation="INSERT" if existing is None else "UPDATE",
            row_key=str(predecessor["incident_id"]),
            sql=(
                "INSERT INTO neg_risk_incident_replay_anchors("
                "incident_id,sequence,scope,kind,state,occurred_at_ms,"
                "evidence_json,recovery_occurred_at_ms,"
                "recovery_evidence_json,row_hash"
                ") VALUES(?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(incident_id) DO UPDATE SET "
                "sequence=excluded.sequence,scope=excluded.scope,"
                "kind=excluded.kind,state=excluded.state,"
                "occurred_at_ms=excluded.occurred_at_ms,"
                "evidence_json=excluded.evidence_json,"
                "recovery_occurred_at_ms=excluded.recovery_occurred_at_ms,"
                "recovery_evidence_json=excluded.recovery_evidence_json,"
                "row_hash=excluded.row_hash"
            ),
            parameters=(
                predecessor["incident_id"],
                predecessor["sequence"],
                predecessor["scope"],
                predecessor["kind"],
                predecessor["state"],
                predecessor["occurred_at_ms"],
                predecessor["evidence_json"],
                recovery_occurred_at_ms,
                recovery_evidence_json,
                row_hash,
            ),
        )

    def _compact_events(
        self,
        con: sqlite3.Connection,
        owner_batch: dict[str, Any] | None,
    ) -> None:
        count = int(
            con.execute("SELECT COUNT(*) FROM neg_risk_incident_events").fetchone()[0]
        )
        if count <= 512:
            return
        rows = con.execute(
            "SELECT * FROM neg_risk_incident_events ORDER BY id LIMIT ?",
            (count - 256,),
        ).fetchall()
        checkpoint = con.execute(
            "SELECT * FROM neg_risk_incident_authority_checkpoint WHERE id=1"
        ).fetchone()
        prefix_hash = None if checkpoint is None else str(checkpoint["prefix_hash"])
        scope_counts: dict[str, tuple[int, int, str]] = {}
        for row in rows:
            payload = {key: row[key] for key in row.keys()}
            payload["previous_hash"] = prefix_hash
            _, prefix_hash = self._row_hash(payload)
            scope = str(row["scope"])
            prior = scope_counts.get(scope)
            scope_counts[scope] = (
                int(row["id"]),
                1 if prior is None else prior[1] + 1,
                prefix_hash,
            )
        through = int(rows[-1]["id"])
        compacted = len(rows) + (
            0 if checkpoint is None else int(checkpoint["compacted_event_count"])
        )
        generation = 1 if checkpoint is None else int(checkpoint["generation"]) + 1
        checkpoint_payload = {
            "compacted_event_count": compacted,
            "generation": generation,
            "prefix_hash": prefix_hash,
            "through_event_id": through,
        }
        _, checkpoint_hash = self._row_hash(checkpoint_payload)
        self._execute_owner_mutation(
            con,
            owner_batch,
            table_name="neg_risk_incident_authority_checkpoint",
            operation="INSERT" if checkpoint is None else "UPDATE",
            row_key="1",
            sql=(
                "INSERT INTO neg_risk_incident_authority_checkpoint("
                "id,generation,through_event_id,compacted_event_count,"
                "prefix_hash,checkpoint_hash) VALUES(1,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "generation=excluded.generation,"
                "through_event_id=excluded.through_event_id,"
                "compacted_event_count=excluded.compacted_event_count,"
                "prefix_hash=excluded.prefix_hash,"
                "checkpoint_hash=excluded.checkpoint_hash"
            ),
            parameters=(generation, through, compacted, prefix_hash, checkpoint_hash),
        )
        for scope, (event_id, deleted_count, floor_hash) in scope_counts.items():
            floor_exists = con.execute(
                "SELECT 1 FROM neg_risk_incident_scope_floors WHERE scope=?",
                (scope,),
            ).fetchone()
            self._execute_owner_mutation(
                con,
                owner_batch,
                table_name="neg_risk_incident_scope_floors",
                operation="INSERT" if floor_exists is None else "UPDATE",
                row_key=scope,
                sql=(
                    "INSERT INTO neg_risk_incident_scope_floors("
                    "scope,through_event_id,compacted_event_count,floor_hash"
                    ") VALUES(?,?,?,?) ON CONFLICT(scope) DO UPDATE SET "
                    "through_event_id=excluded.through_event_id,"
                    "compacted_event_count="
                    "neg_risk_incident_scope_floors.compacted_event_count+"
                    "excluded.compacted_event_count,"
                    "floor_hash=excluded.floor_hash"
                ),
                parameters=(scope, event_id, deleted_count, floor_hash),
            )
        deleted_by_identity = {
            (str(row["incident_id"]), int(row["sequence"])): row
            for row in rows
        }
        existing_anchors = {
            str(row["incident_id"]): row
            for row in con.execute(
                "SELECT * FROM neg_risk_incident_replay_anchors"
            ).fetchall()
        }
        retained_first_rows = con.execute(
            "SELECT e.* FROM neg_risk_incident_events e JOIN ("
            "SELECT incident_id,MIN(id) AS first_id "
            "FROM neg_risk_incident_events WHERE id>? GROUP BY incident_id"
            ") first ON first.first_id=e.id",
            (through,),
        ).fetchall()
        for first in retained_first_rows:
            sequence = int(first["sequence"])
            if sequence <= 1:
                continue
            incident_id = str(first["incident_id"])
            predecessor = deleted_by_identity.get((incident_id, sequence - 1))
            if predecessor is None:
                continue
            prior_anchor = existing_anchors.get(incident_id)
            recovery_at = (
                None
                if prior_anchor is None
                else prior_anchor["recovery_occurred_at_ms"]
            )
            recovery_json = (
                None
                if prior_anchor is None
                else prior_anchor["recovery_evidence_json"]
            )
            for deleted in rows:
                if (
                    str(deleted["incident_id"]) == incident_id
                    and int(deleted["sequence"]) <= int(predecessor["sequence"])
                    and deleted["state"] == "recovering"
                ):
                    recovery_at = int(deleted["occurred_at_ms"])
                    recovery_json = str(deleted["evidence_json"])
            self._upsert_replay_anchor(
                con,
                predecessor,
                recovery_occurred_at_ms=recovery_at,
                recovery_evidence_json=recovery_json,
                owner_batch=owner_batch,
            )
        orphan_anchors = con.execute(
            "SELECT incident_id FROM neg_risk_incident_replay_anchors a "
            "WHERE NOT EXISTS("
            "SELECT 1 FROM neg_risk_incident_events e "
            "WHERE e.incident_id=a.incident_id AND e.id>?"
            ")",
            (through,),
        ).fetchall()
        for anchor in orphan_anchors:
            incident_id = str(anchor["incident_id"])
            self._execute_owner_mutation(
                con,
                owner_batch,
                table_name="neg_risk_incident_replay_anchors",
                operation="DELETE",
                row_key=incident_id,
                sql=(
                    "DELETE FROM neg_risk_incident_replay_anchors "
                    "WHERE incident_id=?"
                ),
                parameters=(incident_id,),
            )
        con.execute(
            "DELETE FROM neg_risk_incident_events WHERE id<=?",
            (through,),
        )

    def _bootstrap_v4_authority(self, con: sqlite3.Connection) -> None:
        for table in (
            "neg_risk_incident_authority_checkpoint",
            "neg_risk_incident_open_authority",
            "neg_risk_incident_open_aggregate",
            "neg_risk_incident_scope_floors",
            "neg_risk_incident_replay_anchors",
        ):
            if con.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None:
                raise ValueError("invalid-incident-migration-target")
        histories: dict[str, list[Incident]] = {}
        rows = con.execute(
            "SELECT * FROM neg_risk_incident_events ORDER BY incident_id,sequence"
        ).fetchall()
        for row in rows:
            event = self._from_row(row)
            history = histories.setdefault(event.id, [])
            if not history:
                if event.sequence != 1 or event.state != "detected":
                    raise ValueError("invalid-incident-history")
            else:
                previous = history[-1]
                if (
                    event.sequence != previous.sequence + 1
                    or event.scope != previous.scope
                    or event.kind != previous.kind
                    or event.state not in ALLOWED[previous.state]
                    or event.occurred_at_ms < previous.occurred_at_ms
                ):
                    raise ValueError("invalid-incident-history")
            history.append(event)
            self._sync_open_authority(con, row, None)
        self._compact_events(con, None)

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
                current_group_row = self._store._current_group_row(con, group_id)
                group = (
                    None
                    if current_group_row is None
                    else self._store._validated_group_from_row(current_group_row)
                )
                current_quote_row = self._store._current_quote_row(
                    con,
                    group_id,
                    verification_at_ms,
                    max(1, verification_at_ms + 1),
                )
                quote = (
                    None
                    if current_quote_row is None or group is None
                    else self._store._validated_quote_from_row(
                        current_quote_row,
                        group,
                        prefix="quote_",
                    )
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
                self._store.discovery_status(
                    row["finished_at_ms"],
                    _connection=con,
                )
            except (sqlite3.Error, TypeError, ValueError):
                return False
            return bool(row["completed"] or row["next_cursor"] != row["requested_cursor"])
        if scope == "reconciliation":
            row = con.execute(
                "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
                (verification_evidence.get("window_id"),),
            ).fetchone()
            try:
                validated = self._store.current_reconciliation(_connection=con)
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
