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
INCIDENT_EVIDENCE_MAX_BYTES = 4_096
INCIDENT_OPEN_AUTHORITY_MAX_ROWS = 4_096
INCIDENT_SCOPE_FLOOR_MAX_ROWS = 8_192


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


@dataclass(frozen=True)
class IncidentPageItem:
    incident: Incident
    detected_at_ms: int
    recovery_occurred_at_ms: int | None
    recovery_evidence: dict[str, Any] | None
    history_floor_event_id: int | None
    history_floor_compacted_count: int | None


@dataclass(frozen=True)
class IncidentPage:
    items: tuple[IncidentPageItem, ...]
    next_before: tuple[int, str] | None
    open_count: int


@dataclass(frozen=True)
class IncidentScopeHistoryItem:
    event_id: int
    incident: Incident


@dataclass(frozen=True)
class IncidentScopeHistoryPage:
    items: tuple[IncidentScopeHistoryItem, ...]
    next_before_event_id: int | None
    floor_event_id: int | None
    floor_compacted_count: int | None


@dataclass(frozen=True)
class IncidentIdentityHistory:
    items: tuple[IncidentScopeHistoryItem, ...]
    history_complete: bool


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
        evidence_json = self._json(evidence)
        now_ms = self._clock_ms()
        con = self._connect()
        authority_mutation_started = False
        try:
            con.execute("BEGIN IMMEDIATE")
            self._validate_and_recover_writer(con, now_ms)
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
            authority_mutation_started = True
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
                    evidence_json,
                ),
            )
            row = con.execute(
                "SELECT * FROM neg_risk_incident_events WHERE id=last_insert_rowid()"
            ).fetchone()
            owner_batch = self._new_owner_batch()
            self._sync_open_authority(con, row, owner_batch)
            compacted = self._compact_events(con, owner_batch)
            self._sync_suffix_authority(
                con,
                owner_batch,
                appended_event=None if compacted else row,
            )
            self._finalize_owner_batch(con, owner_batch)
            con.commit()
            return self._from_row(row)
        except BaseException as error:
            con.rollback()
            if authority_mutation_started and isinstance(
                error,
                (sqlite3.Error, TypeError, ValueError),
            ):
                self._record_evidence_failure(now_ms)
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
        authority_mutation_started = False
        try:
            con.execute("BEGIN IMMEDIATE")
            self._validate_and_recover_writer(con, now_ms)
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
            evidence_json = self._json(evidence)
            authority_mutation_started = True
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
                    evidence_json,
                ),
            )
            written = con.execute(
                "SELECT * FROM neg_risk_incident_events WHERE id=last_insert_rowid()"
            ).fetchone()
            owner_batch = self._new_owner_batch()
            self._sync_open_authority(con, written, owner_batch)
            compacted = self._compact_events(con, owner_batch)
            self._sync_suffix_authority(
                con,
                owner_batch,
                appended_event=None if compacted else written,
            )
            self._finalize_owner_batch(con, owner_batch)
            con.commit()
            return self._from_row(written)
        except BaseException as error:
            con.rollback()
            if authority_mutation_started and isinstance(
                error,
                (sqlite3.Error, TypeError, ValueError),
            ):
                self._record_evidence_failure(now_ms)
            raise
        finally:
            con.close()

    def open_incidents(
        self,
        *,
        _connection: sqlite3.Connection | None = None,
    ) -> tuple[Incident, ...]:
        con = _connection or self._connect(read_only=True)
        try:
            if _connection is None:
                con.execute("BEGIN")
            self._store._assert_owner_journal_clean(con)
            self._validate_checkpoint(con)
            self._validate_bounded_suffix(con)
            self._validate_evidence_failure(con, require_resolved=True)
            aggregate = self._validated_open_aggregate(con)
            rows = con.execute(
                "SELECT incident_id,sequence,scope,kind,state,detected_at_ms,"
                "occurred_at_ms,"
                "evidence_json,recovery_occurred_at_ms,recovery_evidence_json,"
                "row_hash FROM neg_risk_incident_open_authority "
                "ORDER BY occurred_at_ms,incident_id LIMIT ?",
                (INCIDENT_OPEN_AUTHORITY_MAX_ROWS + 1,),
            ).fetchall()
            incidents = tuple(self._from_row(row) for row in rows)
            digest = 0
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
                    "detected_at_ms": int(row["detected_at_ms"]),
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
            if (
                int(aggregate["open_count"]) != len(incidents)
                or str(aggregate["aggregate_digest"]) != f"{digest:064x}"
            ):
                raise ValueError("invalid-incident-open-authority")
            return incidents
        finally:
            if _connection is None:
                con.close()

    def open_incident_page(
        self,
        *,
        limit: int,
        before: tuple[int, str] | None = None,
        _connection: sqlite3.Connection | None = None,
    ) -> IncidentPage:
        if not 1 <= limit <= 500:
            raise ValueError("invalid-incident-page-limit")
        con = _connection or self._connect(read_only=True)
        try:
            self._store._assert_owner_journal_clean(con)
            self._validate_checkpoint(con)
            self._validate_bounded_suffix(con)
            self._validate_evidence_failure(con, require_resolved=True)
            open_count = self._validated_open_count(con)
            where = ""
            parameters: tuple[Any, ...] = ()
            if before is not None:
                before_ms, before_id = before
                if before_ms < 0 or not before_id:
                    raise ValueError("invalid-incident-page-cursor")
                where = (
                    "WHERE occurred_at_ms<? OR "
                    "(occurred_at_ms=? AND incident_id<?) "
                )
                parameters = (before_ms, before_ms, before_id)
            rows = con.execute(
                "SELECT * FROM neg_risk_incident_open_authority "
                f"{where}ORDER BY occurred_at_ms DESC,incident_id DESC LIMIT ?",
                (*parameters, limit + 1),
            ).fetchall()
            page_rows = rows[:limit]
            scopes = tuple({str(row["scope"]) for row in page_rows})
            floors: dict[str, sqlite3.Row] = {}
            if scopes:
                placeholders = ",".join("?" for _ in scopes)
                floors = {
                    str(row["scope"]): self._validate_scope_floor_row(row)
                    for row in con.execute(
                        "SELECT * FROM neg_risk_incident_scope_floors "
                        f"WHERE scope IN ({placeholders})",
                        scopes,
                    ).fetchall()
                }
            items = []
            for row in page_rows:
                incident = self._validate_open_authority_row(row)
                recovery_json = row["recovery_evidence_json"]
                floor = floors.get(incident.scope)
                items.append(
                    IncidentPageItem(
                        incident=incident,
                        detected_at_ms=int(row["detected_at_ms"]),
                        recovery_occurred_at_ms=row["recovery_occurred_at_ms"],
                        recovery_evidence=(
                            None
                            if recovery_json is None
                            else json.loads(str(recovery_json))
                        ),
                        history_floor_event_id=(
                            None if floor is None else int(floor["through_event_id"])
                        ),
                        history_floor_compacted_count=(
                            None
                            if floor is None
                            else int(floor["compacted_event_count"])
                        ),
                    )
                )
            if items and open_count == 0:
                raise ValueError("invalid-incident-open-aggregate")
            next_before = (
                None
                if len(rows) <= limit
                else (
                    int(page_rows[-1]["occurred_at_ms"]),
                    str(page_rows[-1]["incident_id"]),
                )
            )
            return IncidentPage(
                items=tuple(items),
                next_before=next_before,
                open_count=open_count,
            )
        finally:
            if _connection is None:
                con.close()

    def open_incident_status(
        self,
        *,
        _connection: sqlite3.Connection | None = None,
    ) -> tuple[int, bool, bool, bool]:
        con = _connection or self._connect(read_only=True)
        try:
            self._store._assert_owner_journal_clean(con)
            self._validate_checkpoint(con)
            self._validate_bounded_suffix(con)
            self._validate_evidence_failure(con, require_resolved=True)
            open_count = self._validated_open_count(con)
            candidate = con.execute(
                "SELECT * FROM neg_risk_incident_open_authority "
                "WHERE scope='candidate' OR "
                "(scope>='candidate:' AND scope<'candidate;') "
                "ORDER BY scope,kind,occurred_at_ms DESC,incident_id DESC LIMIT 1"
            ).fetchone()
            if candidate is not None:
                self._validate_open_authority_row(candidate)
            http = con.execute(
                "SELECT * FROM neg_risk_incident_open_authority "
                "WHERE scope='http' "
                "ORDER BY scope,kind,occurred_at_ms DESC,incident_id DESC LIMIT 1"
            ).fetchone()
            if http is not None:
                self._validate_open_authority_row(http)
            other = con.execute(
                "SELECT * FROM neg_risk_incident_open_authority "
                "WHERE scope!='http' AND scope!='candidate' AND "
                "NOT (scope>='candidate:' AND scope<'candidate;') "
                "ORDER BY scope,kind,occurred_at_ms DESC,incident_id DESC LIMIT 1"
            ).fetchone()
            if other is not None:
                self._validate_open_authority_row(other)
            return (
                open_count,
                candidate is not None,
                http is not None,
                other is not None,
            )
        finally:
            if _connection is None:
                con.close()

    def group_incident_history(
        self,
        group_id: str,
        *,
        limit: int,
        before_event_id: int | None = None,
        before_order_key: tuple[int, int] | None = None,
        _connection: sqlite3.Connection | None = None,
    ) -> IncidentScopeHistoryPage:
        if not group_id or not 1 <= limit <= 500:
            raise ValueError("invalid-group-incident-history-request")
        if before_event_id is not None and before_event_id <= 0:
            raise ValueError("invalid-group-incident-history-request")
        if before_order_key is not None and (
            before_order_key[0] < 0 or before_order_key[1] <= 0
        ):
            raise ValueError("invalid-group-incident-history-request")
        if before_event_id is not None and before_order_key is not None:
            raise ValueError("invalid-group-incident-history-request")
        scope = f"candidate:{group_id}"
        con = _connection or self._connect(read_only=True)
        try:
            self._store._assert_owner_journal_clean(con)
            self._validate_checkpoint(con)
            self._validate_bounded_suffix(con)
            self._validate_evidence_failure(con, require_resolved=True)
            where = "scope=? "
            parameters: tuple[Any, ...] = (scope,)
            if before_order_key is not None:
                before_ms, before_id = before_order_key
                where += (
                    "AND (occurred_at_ms<? OR "
                    "(occurred_at_ms=? AND id<?)) "
                )
                parameters = (scope, before_ms, before_ms, before_id)
            elif before_event_id is not None:
                where += "AND id<? "
                parameters = (scope, before_event_id)
            order_by = (
                "occurred_at_ms DESC,id DESC"
                if before_order_key is not None
                else "id DESC"
            )
            rows = con.execute(
                "SELECT * FROM neg_risk_incident_events "
                f"WHERE {where}ORDER BY {order_by} LIMIT ?",
                (*parameters, limit + 1),
            ).fetchall()
            page_rows = rows[:limit]
            floor = con.execute(
                "SELECT scope,through_event_id,compacted_event_count,"
                "floor_hash,row_hash "
                "FROM neg_risk_incident_scope_floors WHERE scope=?",
                (scope,),
            ).fetchone()
            if floor is not None:
                floor = self._validate_scope_floor_row(floor)
            return IncidentScopeHistoryPage(
                items=tuple(
                    IncidentScopeHistoryItem(
                        event_id=int(row["id"]),
                        incident=self._from_row(row),
                    )
                    for row in page_rows
                ),
                next_before_event_id=(
                    None
                    if len(rows) <= limit
                    else int(page_rows[-1]["id"])
                ),
                floor_event_id=(
                    None if floor is None else int(floor["through_event_id"])
                ),
                floor_compacted_count=(
                    None
                    if floor is None
                    else int(floor["compacted_event_count"])
                ),
            )
        finally:
            if _connection is None:
                con.close()

    def incident_history(
        self,
        incident_id: str,
        *,
        limit: int = 100,
        _connection: sqlite3.Connection | None = None,
    ) -> IncidentIdentityHistory | None:
        if (
            len(incident_id) != 32
            or any(character not in "0123456789abcdef" for character in incident_id)
            or not 1 <= limit <= 100
        ):
            raise ValueError("invalid-incident-history-request")
        con = _connection or self._connect(read_only=True)
        try:
            self._store._assert_owner_journal_clean(con)
            self._validate_checkpoint(con)
            self._validate_bounded_suffix(con)
            self._validate_evidence_failure(con, require_resolved=True)
            rows = con.execute(
                "SELECT * FROM neg_risk_incident_events "
                "WHERE incident_id=? ORDER BY sequence DESC LIMIT ?",
                (incident_id, limit + 1),
            ).fetchall()
            if not rows:
                return None
            retained = rows[:limit]
            retained.reverse()
            items = tuple(
                IncidentScopeHistoryItem(
                    event_id=int(row["id"]),
                    incident=self._from_row(row),
                )
                for row in retained
            )
            return IncidentIdentityHistory(
                items=items,
                history_complete=(
                    len(rows) <= limit
                    and items[0].incident.sequence == 1
                    and items[0].incident.state == "detected"
                ),
            )
        finally:
            if _connection is None:
                con.close()

    def recent_incidents(
        self,
        scope: str,
        *,
        after_ms: int,
        limit: int,
        _connection: sqlite3.Connection | None = None,
    ) -> tuple[Incident, ...]:
        if (
            not scope
            or len(scope) > 128
            or after_ms < 0
            or not 1 <= limit <= 500
        ):
            raise ValueError("invalid-recent-incident-request")
        con = _connection or self._connect(read_only=True)
        try:
            self._store._assert_owner_journal_clean(con)
            self._validate_checkpoint(con)
            self._validate_bounded_suffix(con)
            self._validate_evidence_failure(con, require_resolved=True)
            rows = con.execute(
                "SELECT e.* FROM neg_risk_incident_events e JOIN ("
                "SELECT incident_id,MAX(sequence) AS latest_sequence "
                "FROM neg_risk_incident_events "
                "WHERE scope=? AND occurred_at_ms>=? GROUP BY incident_id"
                ") latest ON latest.incident_id=e.incident_id "
                "AND latest.latest_sequence=e.sequence "
                "ORDER BY e.occurred_at_ms DESC,e.incident_id DESC LIMIT ?",
                (scope, after_ms, limit),
            ).fetchall()
            return tuple(self._from_row(row) for row in rows)
        finally:
            if _connection is None:
                con.close()

    def _validated_open_count(self, con: sqlite3.Connection) -> int:
        return int(self._validated_open_aggregate(con)["open_count"])

    def _validated_open_digest(self, con: sqlite3.Connection) -> str:
        return str(self._validated_open_aggregate(con)["aggregate_digest"])

    def _validated_open_aggregate(
        self,
        con: sqlite3.Connection,
    ) -> sqlite3.Row:
        aggregate = con.execute(
            "SELECT open_count,aggregate_digest,row_hash FROM "
            "neg_risk_incident_open_aggregate WHERE id=1"
        ).fetchone()
        if aggregate is None:
            raise ValueError("invalid-incident-open-aggregate")
        open_count = int(aggregate["open_count"])
        aggregate_digest = str(aggregate["aggregate_digest"])
        actual_open_count = self._bounded_count(
            con,
            "neg_risk_incident_open_authority",
            INCIDENT_OPEN_AUTHORITY_MAX_ROWS,
        )
        _, expected_hash = self._row_hash(
            {
                "aggregate_digest": aggregate_digest,
                "open_count": open_count,
            }
        )
        if (
            open_count < 0
            or actual_open_count > INCIDENT_OPEN_AUTHORITY_MAX_ROWS
            or open_count != actual_open_count
            or len(aggregate_digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in aggregate_digest
            )
            or str(aggregate["row_hash"]) != expected_hash
        ):
            raise ValueError("invalid-incident-open-aggregate")
        return aggregate

    def _validate_scope_floor_row(self, row: sqlite3.Row) -> sqlite3.Row:
        payload = {
            "compacted_event_count": int(row["compacted_event_count"]),
            "floor_hash": str(row["floor_hash"]),
            "scope": str(row["scope"]),
            "through_event_id": int(row["through_event_id"]),
        }
        _, expected_hash = self._row_hash(payload)
        if (
            not payload["scope"]
            or payload["through_event_id"] <= 0
            or payload["compacted_event_count"] <= 0
            or not payload["floor_hash"].startswith("sha256:")
            or str(row["row_hash"]) != expected_hash
        ):
            raise ValueError("invalid-incident-scope-floor")
        return row

    def _validate_checkpoint(self, con: sqlite3.Connection) -> None:
        suffix_count = self._bounded_count(
            con,
            "neg_risk_incident_events",
            512,
        )
        anchor_count = self._bounded_count(
            con,
            "neg_risk_incident_replay_anchors",
            256,
        )
        if suffix_count > 512 or anchor_count > 256:
            raise ValueError("invalid-incident-checkpoint")
        checkpoint = con.execute(
            "SELECT * FROM neg_risk_incident_authority_checkpoint WHERE id=1"
        ).fetchone()
        floor_count = self._bounded_count(
            con,
            "neg_risk_incident_scope_floors",
            INCIDENT_SCOPE_FLOOR_MAX_ROWS,
        )
        if checkpoint is None:
            if floor_count or anchor_count:
                raise ValueError("invalid-incident-checkpoint")
            return
        if floor_count > INCIDENT_SCOPE_FLOOR_MAX_ROWS:
            raise ValueError("incident-scope-floor-cap")
        payload = {
            "compacted_event_count": int(checkpoint["compacted_event_count"]),
            "generation": int(checkpoint["generation"]),
            "prefix_hash": str(checkpoint["prefix_hash"]),
            "scope_floor_count": int(checkpoint["scope_floor_count"]),
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
            or int(checkpoint["scope_floor_count"]) != floor_count
            or not str(checkpoint["prefix_hash"]).startswith("sha256:")
            or suffix_before_floor is not None
            or invalid_floor is not None
        ):
            raise ValueError("invalid-incident-checkpoint")

    def _validate_writer_authority(self, con: sqlite3.Connection) -> None:
        self._store._assert_owner_journal_clean(con)
        self._validate_checkpoint(con)
        self._validate_bounded_suffix(con)
        self._validated_open_count(con)
        self._validate_evidence_failure(con, require_resolved=False)

    def _validate_and_recover_writer(
        self,
        con: sqlite3.Connection,
        now_ms: int,
    ) -> None:
        try:
            self._validate_writer_authority(con)
            owner_batch = self._new_owner_batch()
            recovered = self._recover_evidence_failure(
                con,
                owner_batch,
                now_ms,
            )
            self._finalize_owner_batch(con, owner_batch)
        except (sqlite3.Error, TypeError, ValueError):
            con.rollback()
            self._record_evidence_failure(now_ms)
            raise
        if not recovered:
            return
        con.commit()
        con.execute("BEGIN IMMEDIATE")
        try:
            self._validate_writer_authority(con)
        except (sqlite3.Error, TypeError, ValueError):
            con.rollback()
            self._record_evidence_failure(now_ms)
            raise

    def _validate_evidence_failure(
        self,
        con: sqlite3.Connection,
        *,
        require_resolved: bool,
    ) -> sqlite3.Row | None:
        row = con.execute(
            "SELECT * FROM neg_risk_evidence_failures WHERE component='incident'"
        ).fetchone()
        if row is None:
            return None
        payload = {
            "component": "incident",
            "failed_at_ms": int(row["failed_at_ms"]),
            "reason": str(row["reason"]),
            "recovered_at_ms": row["recovered_at_ms"],
        }
        _, expected_hash = self._row_hash(payload)
        if (
            str(row["row_hash"]) != expected_hash
            or row["reason"] != "authority-invalid"
            or (
                row["recovered_at_ms"] is not None
                and int(row["recovered_at_ms"]) < int(row["failed_at_ms"])
            )
        ):
            raise ValueError("invalid-incident-evidence-failure")
        if require_resolved and row["recovered_at_ms"] is None:
            raise ValueError("unresolved-incident-evidence-failure")
        return row

    def _record_evidence_failure(self, failed_at_ms: int) -> None:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute(
                "SELECT 1 FROM neg_risk_evidence_failures "
                "WHERE component='incident'"
            ).fetchone()
            payload = {
                "component": "incident",
                "failed_at_ms": failed_at_ms,
                "reason": "authority-invalid",
                "recovered_at_ms": None,
            }
            _, row_hash = self._row_hash(payload)
            batch = self._new_owner_batch()
            self._execute_owner_mutation(
                con,
                batch,
                table_name="neg_risk_evidence_failures",
                operation="INSERT" if existing is None else "UPDATE",
                row_key="incident",
                sql=(
                    "INSERT INTO neg_risk_evidence_failures("
                    "component,failed_at_ms,reason,recovered_at_ms,row_hash"
                    ") VALUES('incident',?,'authority-invalid',NULL,?) "
                    "ON CONFLICT(component) DO UPDATE SET "
                    "failed_at_ms=excluded.failed_at_ms,"
                    "reason=excluded.reason,recovered_at_ms=NULL,"
                    "row_hash=excluded.row_hash"
                ),
                parameters=(failed_at_ms, row_hash),
            )
            self._finalize_owner_batch(con, batch)
            con.commit()
        except (sqlite3.Error, TypeError, ValueError):
            con.rollback()
        finally:
            con.close()

    def _recover_evidence_failure(
        self,
        con: sqlite3.Connection,
        owner_batch: dict[str, Any],
        recovered_at_ms: int,
    ) -> bool:
        row = self._validate_evidence_failure(con, require_resolved=False)
        if row is None or row["recovered_at_ms"] is not None:
            return False
        payload = {
            "component": "incident",
            "failed_at_ms": int(row["failed_at_ms"]),
            "reason": str(row["reason"]),
            "recovered_at_ms": recovered_at_ms,
        }
        _, row_hash = self._row_hash(payload)
        self._execute_owner_mutation(
            con,
            owner_batch,
            table_name="neg_risk_evidence_failures",
            operation="UPDATE",
            row_key="incident",
            sql=(
                "UPDATE neg_risk_evidence_failures SET recovered_at_ms=?,"
                "row_hash=? WHERE component='incident'"
            ),
            parameters=(recovered_at_ms, row_hash),
        )
        return True

    def _validate_bounded_suffix(self, con: sqlite3.Connection) -> None:
        self._validate_suffix_authority(con)
        incident_ids = tuple(
            str(row["incident_id"])
            for row in con.execute(
                "SELECT DISTINCT incident_id FROM neg_risk_incident_events"
            ).fetchall()
        )
        authority_by_id: dict[str, sqlite3.Row] = {}
        if incident_ids:
            placeholders = ",".join("?" for _ in incident_ids)
            rows = con.execute(
                "SELECT * FROM neg_risk_incident_open_authority "
                f"WHERE incident_id IN ({placeholders})",
                incident_ids,
            ).fetchall()
            for row in rows:
                self._validate_open_authority_row(row)
                authority_by_id[str(row["incident_id"])] = row
        self._validate_retained_suffix(con, authority_by_id)

    def _suffix_chain_values(
        self,
        con: sqlite3.Connection,
    ) -> tuple[int, int | None, int | None, str]:
        checkpoint = con.execute(
            "SELECT prefix_hash FROM neg_risk_incident_authority_checkpoint "
            "WHERE id=1"
        ).fetchone()
        previous_hash = (
            "sha256:" + ("0" * 64)
            if checkpoint is None
            else str(checkpoint["prefix_hash"])
        )
        rows = con.execute(
            "SELECT id,incident_id,sequence,scope,kind,state,occurred_at_ms,"
            "evidence_json FROM neg_risk_incident_events ORDER BY id"
        ).fetchall()
        for row in rows:
            previous_hash = self._suffix_event_hash(row, previous_hash)
        return (
            len(rows),
            None if not rows else int(rows[0]["id"]),
            None if not rows else int(rows[-1]["id"]),
            previous_hash,
        )

    def _suffix_event_hash(
        self,
        row: sqlite3.Row,
        previous_hash: str,
    ) -> str:
        payload = {
            "evidence_json": str(row["evidence_json"]),
            "event_id": int(row["id"]),
            "incident_id": str(row["incident_id"]),
            "kind": str(row["kind"]),
            "occurred_at_ms": int(row["occurred_at_ms"]),
            "previous_hash": previous_hash,
            "scope": str(row["scope"]),
            "sequence": int(row["sequence"]),
            "state": str(row["state"]),
        }
        return self._row_hash(payload)[1]

    def _validate_suffix_authority(self, con: sqlite3.Connection) -> None:
        authority = con.execute(
            "SELECT event_count,first_event_id,last_event_id,chain_hash "
            "FROM neg_risk_incident_suffix_authority WHERE id=1"
        ).fetchone()
        if authority is None:
            raise ValueError("invalid-incident-suffix-authority")
        expected = self._suffix_chain_values(con)
        actual = (
            int(authority["event_count"]),
            authority["first_event_id"],
            authority["last_event_id"],
            str(authority["chain_hash"]),
        )
        if actual != expected:
            raise ValueError("invalid-incident-suffix-authority")

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
            "detected_at_ms": int(row["detected_at_ms"]),
            "occurred_at_ms": incident.occurred_at_ms,
            "recovery_evidence": recovery_evidence,
            "recovery_occurred_at_ms": row["recovery_occurred_at_ms"],
            "scope": incident.scope,
            "sequence": incident.sequence,
            "state": incident.state,
        }
        _, expected_hash = self._row_hash(payload)
        if (
            str(row["row_hash"]) != expected_hash
            or int(row["detected_at_ms"]) > incident.occurred_at_ms
        ):
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
    def _bounded_count(
        con: sqlite3.Connection,
        table_name: str,
        maximum: int,
    ) -> int:
        if table_name not in {
            "neg_risk_incident_events",
            "neg_risk_incident_open_authority",
            "neg_risk_incident_replay_anchors",
            "neg_risk_incident_scope_floors",
        }:
            raise ValueError("invalid-incident-count-table")
        return int(
            con.execute(
                f'SELECT COUNT(*) FROM (SELECT 1 FROM "{table_name}" LIMIT ?)',
                (maximum + 1,),
            ).fetchone()[0]
        )

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

    def _sync_suffix_authority(
        self,
        con: sqlite3.Connection,
        owner_batch: dict[str, Any] | None,
        *,
        appended_event: sqlite3.Row | None = None,
    ) -> None:
        authority = con.execute(
            "SELECT event_count,first_event_id,last_event_id,chain_hash "
            "FROM neg_risk_incident_suffix_authority WHERE id=1"
        ).fetchone()
        if appended_event is not None and authority is not None:
            event_id = int(appended_event["id"])
            event_count = int(authority["event_count"]) + 1
            first_event_id = (
                event_id
                if authority["first_event_id"] is None
                else int(authority["first_event_id"])
            )
            last_event_id = event_id
            chain_hash = self._suffix_event_hash(
                appended_event,
                str(authority["chain_hash"]),
            )
        else:
            event_count, first_event_id, last_event_id, chain_hash = (
                self._suffix_chain_values(con)
            )
        self._execute_owner_mutation(
            con,
            owner_batch,
            table_name="neg_risk_incident_suffix_authority",
            operation="INSERT" if authority is None else "UPDATE",
            row_key="1",
            sql=(
                "INSERT INTO neg_risk_incident_suffix_authority("
                "id,event_count,first_event_id,last_event_id,chain_hash"
                ") VALUES(1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
                "event_count=excluded.event_count,"
                "first_event_id=excluded.first_event_id,"
                "last_event_id=excluded.last_event_id,"
                "chain_hash=excluded.chain_hash"
            ),
            parameters=(
                event_count,
                first_event_id,
                last_event_id,
                chain_hash,
            ),
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
            "SELECT open_count,aggregate_digest,row_hash FROM "
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
            if old is None and count >= INCIDENT_OPEN_AUTHORITY_MAX_ROWS:
                raise ValueError("incident-open-authority-cap")
            detected_at = (
                int(event["occurred_at_ms"])
                if old is None
                else int(old["detected_at_ms"])
            )
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
                "detected_at_ms": detected_at,
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
                    "incident_id,sequence,scope,kind,state,detected_at_ms,"
                    "occurred_at_ms,"
                    "evidence_json,recovery_occurred_at_ms,"
                    "recovery_evidence_json,row_hash"
                    ") VALUES(?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(incident_id) DO UPDATE SET "
                    "sequence=excluded.sequence,scope=excluded.scope,"
                    "kind=excluded.kind,state=excluded.state,"
                    "detected_at_ms=excluded.detected_at_ms,"
                    "occurred_at_ms=excluded.occurred_at_ms,"
                    "evidence_json=excluded.evidence_json,"
                    "recovery_occurred_at_ms=excluded.recovery_occurred_at_ms,"
                    "recovery_evidence_json=excluded.recovery_evidence_json,"
                    "row_hash=excluded.row_hash"
                ),
                parameters=(
                    event["incident_id"], event["sequence"], event["scope"],
                    event["kind"], event["state"], detected_at,
                    event["occurred_at_ms"], event["evidence_json"], recovery_at,
                    recovery_json, row_hash,
                ),
            )
            digest ^= int(row_hash.removeprefix("sha256:"), 16)
            if old is None:
                count += 1
        aggregate_digest = f"{digest:064x}"
        _, aggregate_hash = self._row_hash(
            {
                "aggregate_digest": aggregate_digest,
                "open_count": count,
            }
        )
        self._execute_owner_mutation(
            con,
            owner_batch,
            table_name="neg_risk_incident_open_aggregate",
            operation="INSERT" if aggregate is None else "UPDATE",
            row_key="1",
            sql=(
                "INSERT INTO neg_risk_incident_open_aggregate("
                "id,open_count,aggregate_digest,row_hash) VALUES(1,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET open_count=excluded.open_count,"
                "aggregate_digest=excluded.aggregate_digest,"
                "row_hash=excluded.row_hash"
            ),
            parameters=(count, aggregate_digest, aggregate_hash),
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
    ) -> bool:
        count = self._bounded_count(
            con,
            "neg_risk_incident_events",
            512,
        )
        if count <= 512:
            return False
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
        scope_floor_count = self._bounded_count(
            con,
            "neg_risk_incident_scope_floors",
            INCIDENT_SCOPE_FLOOR_MAX_ROWS,
        ) + sum(
            con.execute(
                "SELECT 1 FROM neg_risk_incident_scope_floors WHERE scope=?",
                (scope,),
            ).fetchone()
            is None
            for scope in scope_counts
        )
        if scope_floor_count > INCIDENT_SCOPE_FLOOR_MAX_ROWS:
            raise ValueError("incident-scope-floor-cap")
        checkpoint_payload = {
            "compacted_event_count": compacted,
            "generation": generation,
            "prefix_hash": prefix_hash,
            "scope_floor_count": scope_floor_count,
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
                "scope_floor_count,prefix_hash,checkpoint_hash"
                ") VALUES(1,?,?,?,?,?,?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "generation=excluded.generation,"
                "through_event_id=excluded.through_event_id,"
                "compacted_event_count=excluded.compacted_event_count,"
                "scope_floor_count=excluded.scope_floor_count,"
                "prefix_hash=excluded.prefix_hash,"
                "checkpoint_hash=excluded.checkpoint_hash"
            ),
            parameters=(
                generation,
                through,
                compacted,
                scope_floor_count,
                prefix_hash,
                checkpoint_hash,
            ),
        )
        for scope, (event_id, deleted_count, floor_hash) in scope_counts.items():
            floor = con.execute(
                "SELECT * FROM neg_risk_incident_scope_floors WHERE scope=?",
                (scope,),
            ).fetchone()
            compacted_event_count = deleted_count + (
                0 if floor is None else int(floor["compacted_event_count"])
            )
            floor_payload = {
                "compacted_event_count": compacted_event_count,
                "floor_hash": floor_hash,
                "scope": scope,
                "through_event_id": event_id,
            }
            _, floor_row_hash = self._row_hash(floor_payload)
            self._execute_owner_mutation(
                con,
                owner_batch,
                table_name="neg_risk_incident_scope_floors",
                operation="INSERT" if floor is None else "UPDATE",
                row_key=scope,
                sql=(
                    "INSERT INTO neg_risk_incident_scope_floors("
                    "scope,through_event_id,compacted_event_count,floor_hash,"
                    "row_hash) VALUES(?,?,?,?,?) "
                    "ON CONFLICT(scope) DO UPDATE SET "
                    "through_event_id=excluded.through_event_id,"
                    "compacted_event_count=excluded.compacted_event_count,"
                    "floor_hash=excluded.floor_hash,"
                    "row_hash=excluded.row_hash"
                ),
                parameters=(
                    scope,
                    event_id,
                    compacted_event_count,
                    floor_hash,
                    floor_row_hash,
                ),
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
        return True

    def _bootstrap_v4_authority(self, con: sqlite3.Connection) -> None:
        for table in (
            "neg_risk_incident_authority_checkpoint",
            "neg_risk_incident_open_authority",
            "neg_risk_incident_open_aggregate",
            "neg_risk_incident_scope_floors",
            "neg_risk_incident_suffix_authority",
            "neg_risk_incident_replay_anchors",
        ):
            if con.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None:
                raise ValueError("invalid-incident-migration-target")
        histories: dict[str, list[Incident]] = {}
        rows = con.execute(
            "SELECT * FROM neg_risk_incident_events "
            "ORDER BY incident_id,sequence LIMIT ?",
            (513,),
        ).fetchall()
        if len(rows) > 512:
            raise ValueError("incident-event-suffix-cap")
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
        if not rows:
            aggregate_digest = "0" * 64
            _, aggregate_hash = self._row_hash(
                {
                    "aggregate_digest": aggregate_digest,
                    "open_count": 0,
                }
            )
            con.execute(
                "INSERT INTO neg_risk_incident_open_aggregate("
                "id,open_count,aggregate_digest,row_hash) VALUES(1,0,?,?)",
                (aggregate_digest, aggregate_hash),
            )
        self._sync_suffix_authority(con, None)

    def _migrate_v4_to_v5_authority(self, con: sqlite3.Connection) -> None:
        checkpoint = con.execute(
            "SELECT * FROM neg_risk_incident_authority_checkpoint WHERE id=1"
        ).fetchone()
        if checkpoint is not None:
            old_checkpoint_payload = {
                "compacted_event_count": int(
                    checkpoint["compacted_event_count"]
                ),
                "generation": int(checkpoint["generation"]),
                "prefix_hash": str(checkpoint["prefix_hash"]),
                "through_event_id": int(checkpoint["through_event_id"]),
            }
            if self._row_hash(old_checkpoint_payload)[1] != str(
                checkpoint["checkpoint_hash"]
            ):
                raise ValueError("invalid-v4-incident-checkpoint")
        raw_rows = con.execute(
            "SELECT * FROM neg_risk_incident_events ORDER BY id LIMIT ?",
            (513,),
        ).fetchall()
        if len(raw_rows) > 512:
            raise ValueError("incident-event-suffix-cap")
        for row in raw_rows:
            self._from_row(row)
        open_rows = con.execute(
            "SELECT * FROM neg_risk_incident_open_authority "
            "ORDER BY incident_id LIMIT ?",
            (INCIDENT_OPEN_AUTHORITY_MAX_ROWS + 1,),
        ).fetchall()
        if len(open_rows) > INCIDENT_OPEN_AUTHORITY_MAX_ROWS:
            raise ValueError("incident-open-authority-cap")
        old_digest = 0
        for row in open_rows:
            incident = self._from_row(row)
            recovery_json = row["recovery_evidence_json"]
            recovery_evidence = (
                None if recovery_json is None else json.loads(str(recovery_json))
            )
            old_payload = {
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
            _, expected_hash = self._row_hash(old_payload)
            if str(row["row_hash"]) != expected_hash:
                raise ValueError("invalid-v4-incident-open-authority")
            old_digest ^= int(expected_hash.removeprefix("sha256:"), 16)
        aggregate = con.execute(
            "SELECT open_count,aggregate_digest FROM "
            "neg_risk_incident_open_aggregate WHERE id=1"
        ).fetchone()
        if aggregate is not None and (
            int(aggregate["open_count"]) != len(open_rows)
            or str(aggregate["aggregate_digest"]) != f"{old_digest:064x}"
        ):
            raise ValueError("invalid-v4-incident-open-aggregate")
        if aggregate is None and open_rows:
            raise ValueError("invalid-v4-incident-open-aggregate")
        floor_rows = con.execute(
            "SELECT * FROM neg_risk_incident_scope_floors "
            "ORDER BY scope LIMIT ?",
            (INCIDENT_SCOPE_FLOOR_MAX_ROWS + 1,),
        ).fetchall()
        if len(floor_rows) > INCIDENT_SCOPE_FLOOR_MAX_ROWS:
            raise ValueError("incident-scope-floor-cap")

        con.execute("DROP INDEX idx_neg_risk_incident_open_page")
        con.execute("DROP INDEX idx_neg_risk_incident_open_scope_kind")
        for table in (
            "neg_risk_incident_authority_checkpoint",
            "neg_risk_incident_open_authority",
            "neg_risk_incident_open_aggregate",
            "neg_risk_incident_scope_floors",
        ):
            con.execute(f'ALTER TABLE "{table}" RENAME TO "{table}_v4"')
        migration_ddl = """
            CREATE TABLE neg_risk_incident_authority_checkpoint (
              id INTEGER PRIMARY KEY CHECK(id=1),
              generation INTEGER NOT NULL CHECK(generation >= 1),
              through_event_id INTEGER NOT NULL CHECK(through_event_id >= 0),
              compacted_event_count INTEGER NOT NULL
                CHECK(compacted_event_count >= 0),
              scope_floor_count INTEGER NOT NULL CHECK(scope_floor_count >= 0),
              prefix_hash TEXT NOT NULL,
              checkpoint_hash TEXT NOT NULL
            );
            CREATE TABLE neg_risk_incident_open_authority (
              incident_id TEXT PRIMARY KEY,
              sequence INTEGER NOT NULL CHECK(sequence >= 1),
              scope TEXT NOT NULL,
              kind TEXT NOT NULL,
              state TEXT NOT NULL CHECK(state IN
                ('detected','classified','contained','recovering','escalated')),
              detected_at_ms INTEGER NOT NULL CHECK(detected_at_ms >= 0),
              occurred_at_ms INTEGER NOT NULL CHECK(occurred_at_ms >= 0),
              evidence_json TEXT NOT NULL,
              recovery_occurred_at_ms INTEGER,
              recovery_evidence_json TEXT,
              row_hash TEXT NOT NULL
            );
            CREATE INDEX idx_neg_risk_incident_open_page
              ON neg_risk_incident_open_authority(occurred_at_ms DESC,incident_id DESC);
            CREATE INDEX idx_neg_risk_incident_open_scope_kind
              ON neg_risk_incident_open_authority(
                scope,kind,occurred_at_ms DESC,incident_id DESC);
            CREATE TABLE neg_risk_incident_open_aggregate (
              id INTEGER PRIMARY KEY CHECK(id=1),
              open_count INTEGER NOT NULL CHECK(open_count >= 0),
              aggregate_digest TEXT NOT NULL,
              row_hash TEXT NOT NULL
            );
            CREATE TABLE neg_risk_incident_scope_floors (
              scope TEXT PRIMARY KEY,
              through_event_id INTEGER NOT NULL CHECK(through_event_id > 0),
              compacted_event_count INTEGER NOT NULL
                CHECK(compacted_event_count > 0),
              floor_hash TEXT NOT NULL,
              row_hash TEXT NOT NULL
            );
            CREATE TABLE neg_risk_incident_suffix_authority (
              id INTEGER PRIMARY KEY CHECK(id=1),
              event_count INTEGER NOT NULL CHECK(event_count >= 0),
              first_event_id INTEGER
                CHECK(first_event_id IS NULL OR first_event_id > 0),
              last_event_id INTEGER
                CHECK(last_event_id IS NULL OR last_event_id > 0),
              chain_hash TEXT NOT NULL,
              CHECK(
                (event_count=0 AND first_event_id IS NULL AND last_event_id IS NULL)
                OR
                (event_count>0 AND first_event_id IS NOT NULL
                 AND last_event_id IS NOT NULL)
              )
            );
            """
        for statement in migration_ddl.split(";"):
            if statement.strip():
                con.execute(statement)
        digest = 0
        for row in open_rows:
            incident = self._from_row(row)
            detected_row = con.execute(
                "SELECT MIN(occurred_at_ms) FROM neg_risk_incident_events "
                "WHERE incident_id=?",
                (incident.id,),
            ).fetchone()
            detected_at_ms = (
                incident.occurred_at_ms
                if detected_row[0] is None
                else int(detected_row[0])
            )
            recovery_json = row["recovery_evidence_json"]
            payload = {
                "evidence": incident.evidence,
                "incident_id": incident.id,
                "kind": incident.kind,
                "detected_at_ms": detected_at_ms,
                "occurred_at_ms": incident.occurred_at_ms,
                "recovery_evidence": (
                    None
                    if recovery_json is None
                    else json.loads(str(recovery_json))
                ),
                "recovery_occurred_at_ms": row["recovery_occurred_at_ms"],
                "scope": incident.scope,
                "sequence": incident.sequence,
                "state": incident.state,
            }
            _, row_hash = self._row_hash(payload)
            con.execute(
                "INSERT INTO neg_risk_incident_open_authority("
                "incident_id,sequence,scope,kind,state,detected_at_ms,"
                "occurred_at_ms,evidence_json,recovery_occurred_at_ms,"
                "recovery_evidence_json,row_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                (
                    incident.id,
                    incident.sequence,
                    incident.scope,
                    incident.kind,
                    incident.state,
                    detected_at_ms,
                    incident.occurred_at_ms,
                    row["evidence_json"],
                    row["recovery_occurred_at_ms"],
                    recovery_json,
                    row_hash,
                ),
            )
            digest ^= int(row_hash.removeprefix("sha256:"), 16)
        aggregate_digest = f"{digest:064x}"
        _, aggregate_hash = self._row_hash(
            {
                "aggregate_digest": aggregate_digest,
                "open_count": len(open_rows),
            }
        )
        con.execute(
            "INSERT INTO neg_risk_incident_open_aggregate("
            "id,open_count,aggregate_digest,row_hash) VALUES(1,?,?,?)",
            (len(open_rows), aggregate_digest, aggregate_hash),
        )
        for row in floor_rows:
            payload = {
                "compacted_event_count": int(row["compacted_event_count"]),
                "floor_hash": str(row["floor_hash"]),
                "scope": str(row["scope"]),
                "through_event_id": int(row["through_event_id"]),
            }
            _, row_hash = self._row_hash(payload)
            con.execute(
                "INSERT INTO neg_risk_incident_scope_floors("
                "scope,through_event_id,compacted_event_count,floor_hash,row_hash"
                ") VALUES(?,?,?,?,?)",
                (
                    row["scope"],
                    row["through_event_id"],
                    row["compacted_event_count"],
                    row["floor_hash"],
                    row_hash,
                ),
            )
        if checkpoint is not None:
            checkpoint_payload = {
                "compacted_event_count": int(
                    checkpoint["compacted_event_count"]
                ),
                "generation": int(checkpoint["generation"]),
                "prefix_hash": str(checkpoint["prefix_hash"]),
                "scope_floor_count": len(floor_rows),
                "through_event_id": int(checkpoint["through_event_id"]),
            }
            _, checkpoint_hash = self._row_hash(checkpoint_payload)
            con.execute(
                "INSERT INTO neg_risk_incident_authority_checkpoint("
                "id,generation,through_event_id,compacted_event_count,"
                "scope_floor_count,prefix_hash,checkpoint_hash"
                ") VALUES(1,?,?,?,?,?,?)",
                (
                    checkpoint["generation"],
                    checkpoint["through_event_id"],
                    checkpoint["compacted_event_count"],
                    len(floor_rows),
                    checkpoint["prefix_hash"],
                    checkpoint_hash,
                ),
            )
        self._sync_suffix_authority(con, None)
        for table in (
            "neg_risk_incident_authority_checkpoint_v4",
            "neg_risk_incident_open_authority_v4",
            "neg_risk_incident_open_aggregate_v4",
            "neg_risk_incident_scope_floors_v4",
        ):
            con.execute(f'DROP TABLE "{table}"')

    def _initialize_empty_v4_authority(self, con: sqlite3.Connection) -> None:
        if con.execute(
            "SELECT 1 FROM neg_risk_incident_events LIMIT 1"
        ).fetchone() is not None:
            raise ValueError("invalid-incident-bootstrap")
        aggregate_digest = "0" * 64
        _, aggregate_hash = self._row_hash(
            {
                "aggregate_digest": aggregate_digest,
                "open_count": 0,
            }
        )
        owner_batch = self._new_owner_batch()
        if con.execute(
            "SELECT 1 FROM neg_risk_incident_open_aggregate WHERE id=1"
        ).fetchone() is None:
            self._execute_owner_mutation(
                con,
                owner_batch,
                table_name="neg_risk_incident_open_aggregate",
                operation="INSERT",
                row_key="1",
                sql=(
                    "INSERT INTO neg_risk_incident_open_aggregate("
                    "id,open_count,aggregate_digest,row_hash) VALUES(1,0,?,?)"
                ),
                parameters=(aggregate_digest, aggregate_hash),
            )
        self._sync_suffix_authority(con, owner_batch)
        self._finalize_owner_batch(con, owner_batch)

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
                and (
                    incident.kind
                    not in {
                        "resource-disk-pressure",
                        "resource-contention",
                    }
                    or (
                        decision.mode == "normal"
                        and decision.health_claimed
                    )
                )
            )
        if scope == "capacity":
            receipt_at_ms = verification_evidence.get("last_recovery_receipt_at_ms")
            episode = con.execute(
                "SELECT MIN(occurred_at_ms) AS detected_at_ms "
                "FROM neg_risk_incident_events WHERE incident_id=?",
                (incident.id,),
            ).fetchone()
            detected_at_ms = None if episode is None else episode["detected_at_ms"]
            if (
                type(receipt_at_ms) is not int
                or type(detected_at_ms) is not int
                or receipt_at_ms < detected_at_ms
            ):
                return False
            receipt = con.execute(
                "SELECT completed_at_ms,deleted_count FROM capacity_reclaim_receipts "
                "WHERE completed_at_ms=? ORDER BY id DESC LIMIT 1",
                (receipt_at_ms,),
            ).fetchone()
            runtime = con.execute(
                "SELECT state,last_recovery_receipt_at_ms FROM "
                "capacity_controller_runtime WHERE id=1"
            ).fetchone()
            return bool(
                receipt
                and receipt["deleted_count"] > 0
                and detected_at_ms <= receipt["completed_at_ms"] <= verification_at_ms
                and runtime
                and runtime["state"] == "normal"
                and runtime["last_recovery_receipt_at_ms"] == receipt_at_ms
            )
        if scope == "quote-collection":
            run_id = verification_evidence.get("run_id")
            if type(run_id) is not int or run_id < 1:
                return False
            row = con.execute(
                "SELECT id,status,completed_at_ms,requested_token_count,"
                "successful_response_count FROM neg_risk_quote_runs WHERE id=?",
                (run_id,),
            ).fetchone()
            return bool(
                row
                and row["status"] == "complete"
                and row["completed_at_ms"] is not None
                and recovery_started_at_ms <= row["completed_at_ms"] <= verification_at_ms
                and row["requested_token_count"]
                == verification_evidence.get("requested_token_count")
                and row["successful_response_count"]
                == verification_evidence.get("successful_response_count")
            )
        if scope == "quote":
            run_id = verification_evidence.get("run_id")
            if type(run_id) is not int or run_id < 1:
                return False
            row = con.execute(
                "SELECT id,status,quoted_at_ms,completed_at_ms,requested_token_count,"
                "successful_response_count FROM neg_risk_quote_runs WHERE id=?",
                (run_id,),
            ).fetchone()
            return bool(
                row
                and row["status"] == "complete"
                and row["completed_at_ms"] is not None
                and recovery_started_at_ms <= row["completed_at_ms"] <= verification_at_ms
                and row["quoted_at_ms"] == verification_evidence.get("quote_taken_at_ms")
                and row["requested_token_count"]
                == verification_evidence.get("requested_token_count")
                and row["successful_response_count"]
                == verification_evidence.get("successful_response_count")
            )
        if scope.startswith("notification:"):
            try:
                notification_id = int(scope.split(":", 1)[1])
            except ValueError:
                return False
            failed_attempt_id = recovery_evidence.get("failed_attempt_id")
            delivered_attempt_id = verification_evidence.get(
                "delivered_attempt_id"
            )
            if (
                type(failed_attempt_id) is not int
                or type(delivered_attempt_id) is not int
                or verification_evidence.get("notification_id")
                != notification_id
                or delivered_attempt_id <= failed_attempt_id
            ):
                return False
            row = con.execute(
                "SELECT * FROM neg_risk_opportunity_notification_attempts "
                "WHERE id=? AND notification_id=?",
                (delivered_attempt_id, notification_id),
            ).fetchone()
            return bool(
                row
                and row["outcome"] == "delivered"
                and row["error_kind"] is None
                and row["attempted_at_ms"] >= recovery_started_at_ms
                and row["attempted_at_ms"] <= verification_at_ms
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
            encoded = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
        except (TypeError, ValueError) as error:
            raise ValueError("invalid-incident-evidence") from error
        if len(encoded.encode("utf-8")) > INCIDENT_EVIDENCE_MAX_BYTES:
            raise ValueError("incident-evidence-too-large")
        return encoded

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
                or len(str(row["evidence_json"]).encode("utf-8"))
                > INCIDENT_EVIDENCE_MAX_BYTES
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
    "IncidentIdentityHistory",
    "IncidentManager",
    "InvalidIncidentTransitionError",
    "RecoveryEvidenceRequiredError",
]
