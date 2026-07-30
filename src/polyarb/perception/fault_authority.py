"""Append-only SQLite authority for scoped upstream fault control."""

from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from uuid import UUID

from polyarb.perception.fault_control import (
    FaultAuthorization,
    FaultEvent,
    FaultEventState,
    FaultHistory,
    FaultIntent,
    FaultIntentRequest,
    FaultOwnershipCapability,
    FaultProjection,
    FaultRuntimeIdentity,
    IntentAdmission,
    canonical_digest,
    canonical_json,
    normalize_evidence,
    normalize_identifier,
)

_ZERO_HASH = "0" * 64
_TERMINAL_STATES = frozenset(
    {
        FaultEventState.VERIFIED,
        FaultEventState.REJECTED,
        FaultEventState.EXPIRED,
        FaultEventState.ABANDONED,
        FaultEventState.CLEANUP_FAILED,
        FaultEventState.RECOVERY_TIMEOUT,
        FaultEventState.EVIDENCE_INVALID,
        FaultEventState.ESCALATED,
    }
)
_NEXT_STATES: Mapping[FaultEventState, frozenset[FaultEventState]] = {
    FaultEventState.AUTHORIZED: frozenset(
        {
            FaultEventState.ARMED,
            FaultEventState.REJECTED,
            FaultEventState.EXPIRED,
            FaultEventState.ABANDONED,
            FaultEventState.ESCALATED,
        }
    ),
    FaultEventState.ARMED: frozenset(
        {
            FaultEventState.INJECTED,
            FaultEventState.EXPIRED,
            FaultEventState.ABANDONED,
            FaultEventState.CLEANUP_FAILED,
            FaultEventState.ESCALATED,
        }
    ),
    FaultEventState.INJECTED: frozenset(
        {
            FaultEventState.DETECTED,
            FaultEventState.CLEANUP_FAILED,
            FaultEventState.ESCALATED,
        }
    ),
    FaultEventState.DETECTED: frozenset(
        {
            FaultEventState.CONTAINED,
            FaultEventState.CLEANUP_FAILED,
            FaultEventState.ESCALATED,
        }
    ),
    FaultEventState.CONTAINED: frozenset(
        {
            FaultEventState.CLEANED,
            FaultEventState.CLEANUP_FAILED,
            FaultEventState.ESCALATED,
        }
    ),
    FaultEventState.CLEANED: frozenset(
        {
            FaultEventState.RECOVERED,
            FaultEventState.RECOVERY_TIMEOUT,
            FaultEventState.EVIDENCE_INVALID,
            FaultEventState.ESCALATED,
        }
    ),
    FaultEventState.RECOVERED: frozenset(
        {
            FaultEventState.VERIFIED,
            FaultEventState.EVIDENCE_INVALID,
            FaultEventState.ESCALATED,
        }
    ),
}
_PROCESS_OWNED_STATES = frozenset(
    {
        FaultEventState.INJECTED,
        FaultEventState.CLEANED,
        FaultEventState.RECOVERED,
    }
)


def _runtime_hash(
    identity: FaultRuntimeIdentity,
    *,
    supervisor_run_id: str,
    attempt: int,
    started_at_ms: int,
) -> str:
    return canonical_digest(
        {
            "attempt": attempt,
            "boot_id": str(identity.boot_id),
            "component": identity.component,
            "machine_id": identity.machine_id,
            "release_id": identity.release_id,
            "started_at_ms": started_at_ms,
            "supervisor_run_id": supervisor_run_id,
        }
    )


def _nonce_hash(
    *,
    nonce_digest: str,
    authorization_digest: str,
    accepted_at_ms: int,
) -> str:
    return canonical_digest(
        {
            "accepted_at_ms": accepted_at_ms,
            "authorization_digest": authorization_digest,
            "nonce_digest": nonce_digest,
        }
    )


def _intent_hash(fields: Mapping[str, object]) -> str:
    return canonical_digest(
        {
            "accepted_at_ms": fields["accepted_at_ms"],
            "authorization_digest": fields["authorization_digest"],
            "boot_id": fields["boot_id"],
            "call_class": fields["call_class"],
            "component": fields["component"],
            "fault_id": fields["fault_id"],
            "kind": fields["kind"],
            "machine_id": fields["machine_id"],
            "nonce_digest": fields["nonce_digest"],
            "parameter_digest": fields["parameter_digest"],
            "parameters_json": fields["parameters_json"],
            "rejection_reason": fields["rejection_reason"],
            "release_id": fields["release_id"],
            "status": fields["status"],
            "target_key": fields["target_key"],
            "ttl_ms": fields["ttl_ms"],
        }
    )


def _ownership_digest(capability: FaultOwnershipCapability) -> str:
    return canonical_digest(
        {
            "boot_id": str(capability.runtime.boot_id),
            "component": capability.runtime.component,
            "fault_id": capability.fault_id,
            "machine_id": capability.runtime.machine_id,
            "release_id": capability.runtime.release_id,
            "token": capability.token,
        }
    )


def _event_hash(
    *,
    fault_id: str,
    sequence: int,
    state: FaultEventState,
    action: str | None,
    occurred_at_ms: int,
    evidence_json: str,
    previous_hash: str,
) -> str:
    return canonical_digest(
        {
            "action": action,
            "evidence": json.loads(evidence_json),
            "fault_id": fault_id,
            "occurred_at_ms": occurred_at_ms,
            "previous_hash": previous_hash,
            "sequence": sequence,
            "state": state.value,
        }
    )


class FaultAuthorityStore:
    """Small, independently usable append-only authority boundary."""

    def __init__(
        self,
        db_path: Path,
        *,
        read_only: bool = False,
        busy_timeout_ms: int = 1_000,
    ) -> None:
        if (
            isinstance(busy_timeout_ms, bool)
            or not isinstance(busy_timeout_ms, int)
            or not 1 <= busy_timeout_ms <= 5_000
        ):
            raise ValueError("invalid-busy-timeout")
        self._db_path = Path(db_path)
        self._read_only = read_only
        self._busy_timeout_ms = busy_timeout_ms

    def _connect(self) -> sqlite3.Connection:
        target = (
            f"file:{self._db_path.resolve()}?mode=ro" if self._read_only else str(self._db_path)
        )
        con = sqlite3.connect(
            target,
            uri=self._read_only,
            isolation_level=None,
            timeout=self._busy_timeout_ms / 1_000,
        )
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    @staticmethod
    def _validate_time(value: int, reason: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(reason)

    def register_runtime_start(
        self,
        identity: FaultRuntimeIdentity,
        *,
        supervisor_run_id: str,
        attempt: int,
        started_at_ms: int,
    ) -> FaultRuntimeIdentity:
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        if not isinstance(identity, FaultRuntimeIdentity):
            raise ValueError("invalid-runtime")
        try:
            supervisor_run_id = normalize_identifier(
                supervisor_run_id,
                reason="invalid-supervisor-run-id",
            )
        except ValueError as exc:
            raise ValueError("invalid-supervisor-run-id") from exc
        if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
            raise ValueError("invalid-attempt")
        self._validate_time(started_at_ms, "invalid-started-at")
        digest = _runtime_hash(
            identity,
            supervisor_run_id=supervisor_run_id,
            attempt=attempt,
            started_at_ms=started_at_ms,
        )
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            existing = con.execute(
                "SELECT * FROM neg_risk_fault_runtime_starts "
                "WHERE component=? AND release_id=? AND machine_id=? AND boot_id=?",
                (
                    identity.component,
                    identity.release_id,
                    identity.machine_id,
                    str(identity.boot_id),
                ),
            ).fetchone()
            if existing is None:
                con.execute(
                    "INSERT INTO neg_risk_fault_runtime_starts("
                    "component,release_id,machine_id,boot_id,supervisor_run_id,"
                    "attempt,started_at_ms,identity_digest) VALUES (?,?,?,?,?,?,?,?)",
                    (
                        identity.component,
                        identity.release_id,
                        identity.machine_id,
                        str(identity.boot_id),
                        supervisor_run_id,
                        attempt,
                        started_at_ms,
                        digest,
                    ),
                )
            elif existing["identity_digest"] != digest:
                raise ValueError("runtime-identity-conflict")
            con.execute("COMMIT")
            return identity
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def current_runtime(self, component: str) -> FaultRuntimeIdentity | None:
        try:
            con = self._connect()
            try:
                row = con.execute(
                    "SELECT * "
                    "FROM neg_risk_fault_runtime_starts WHERE component=? "
                    "ORDER BY started_at_ms DESC,id DESC LIMIT 1",
                    (component,),
                ).fetchone()
            finally:
                con.close()
        except sqlite3.Error:
            return None
        if row is None or not self._runtime_row_valid(row):
            return None
        return self._runtime_from_row(row)

    @staticmethod
    def _runtime_from_row(row: sqlite3.Row) -> FaultRuntimeIdentity:
        return FaultRuntimeIdentity(
            component=row["component"],
            release_id=row["release_id"],
            machine_id=row["machine_id"],
            boot_id=UUID(row["boot_id"]),
        )

    @classmethod
    def _runtime_row_valid(cls, row: sqlite3.Row) -> bool:
        try:
            identity = cls._runtime_from_row(row)
            return row["identity_digest"] == _runtime_hash(
                identity,
                supervisor_run_id=row["supervisor_run_id"],
                attempt=row["attempt"],
                started_at_ms=row["started_at_ms"],
            )
        except (KeyError, ValueError, TypeError, IndexError):
            return False

    @staticmethod
    def _intent_from_row(row: sqlite3.Row) -> FaultIntent:
        return FaultIntent(
            fault_id=row["fault_id"],
            kind=row["kind"],
            call_class=row["call_class"],
            target_key=row["target_key"],
            parameters=json.loads(row["parameters_json"]),
            ttl_ms=row["ttl_ms"],
            runtime=FaultRuntimeIdentity(
                component=row["component"],
                release_id=row["release_id"],
                machine_id=row["machine_id"],
                boot_id=UUID(row["boot_id"]),
            ),
            nonce_digest=row["nonce_digest"],
            accepted_at_ms=row["accepted_at_ms"],
        )

    @staticmethod
    def _latest_state(con: sqlite3.Connection, fault_id: str) -> FaultEventState | None:
        row = con.execute(
            "SELECT state FROM neg_risk_fault_events WHERE fault_id=? "
            "ORDER BY sequence DESC LIMIT 1",
            (fault_id,),
        ).fetchone()
        return FaultEventState(row["state"]) if row is not None else None

    def _has_active_chain(self, con: sqlite3.Connection) -> bool:
        rows = con.execute(
            "SELECT * FROM neg_risk_fault_intents WHERE status='accepted'"
        ).fetchall()
        for row in rows:
            current = con.execute(
                "SELECT release_id,machine_id,boot_id FROM neg_risk_fault_runtime_starts "
                "WHERE component=? ORDER BY started_at_ms DESC,id DESC LIMIT 1",
                (row["component"],),
            ).fetchone()
            if current is None:
                continue
            exact = (
                current["release_id"] == row["release_id"]
                and current["machine_id"] == row["machine_id"]
                and current["boot_id"] == row["boot_id"]
            )
            state = self._latest_state(con, row["fault_id"])
            if exact and state not in _TERMINAL_STATES:
                return True
        return False

    def accept_intent(
        self,
        request: FaultIntentRequest,
        *,
        auth: FaultAuthorization,
        accepted_at_ms: int,
    ) -> IntentAdmission:
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        if not isinstance(request, FaultIntentRequest) or not isinstance(auth, FaultAuthorization):
            raise ValueError("invalid-intent-envelope")
        self._validate_time(accepted_at_ms, "invalid-accepted-at")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            replay = con.execute(
                "SELECT 1 FROM neg_risk_fault_auth_nonces WHERE nonce_digest=?",
                (auth.nonce_digest,),
            ).fetchone()
            current = con.execute(
                "SELECT * "
                "FROM neg_risk_fault_runtime_starts WHERE component=? "
                "ORDER BY started_at_ms DESC,id DESC LIMIT 1",
                (request.runtime.component,),
            ).fetchone()
            reason = "accepted"
            if replay is not None:
                reason = "nonce-replay"
            elif current is None or not self._runtime_row_valid(current):
                reason = "runtime-unavailable"
            elif self._runtime_from_row(current) != request.runtime:
                reason = "runtime-mismatch"
            elif self._has_active_chain(con):
                reason = "fault-already-active"

            if replay is None:
                nonce_hash = _nonce_hash(
                    nonce_digest=auth.nonce_digest,
                    authorization_digest=auth.authorization_digest,
                    accepted_at_ms=accepted_at_ms,
                )
                con.execute(
                    "INSERT INTO neg_risk_fault_auth_nonces("
                    "nonce_digest,authorization_digest,accepted_at_ms,row_hash) "
                    "VALUES (?,?,?,?)",
                    (
                        auth.nonce_digest,
                        auth.authorization_digest,
                        accepted_at_ms,
                        nonce_hash,
                    ),
                )
            parameters_json = canonical_json(dict(request.parameters))
            intent_fields: dict[str, object] = {
                "fault_id": request.fault_id,
                "kind": request.kind.value,
                "call_class": request.call_class.value,
                "target_key": request.target_key,
                "parameters_json": parameters_json,
                "parameter_digest": hashlib.sha256(parameters_json.encode()).hexdigest(),
                "ttl_ms": request.ttl_ms,
                "component": request.runtime.component,
                "release_id": request.runtime.release_id,
                "machine_id": request.runtime.machine_id,
                "boot_id": str(request.runtime.boot_id),
                "nonce_digest": auth.nonce_digest,
                "authorization_digest": auth.authorization_digest,
                "accepted_at_ms": accepted_at_ms,
                "status": "accepted" if reason == "accepted" else "rejected",
                "rejection_reason": None if reason == "accepted" else reason,
            }
            con.execute(
                "INSERT INTO neg_risk_fault_intents("
                "fault_id,kind,call_class,target_key,parameters_json,parameter_digest,"
                "ttl_ms,component,release_id,machine_id,boot_id,nonce_digest,"
                "authorization_digest,accepted_at_ms,status,rejection_reason,intent_hash)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    *(
                        intent_fields[key]
                        for key in (
                            "fault_id",
                            "kind",
                            "call_class",
                            "target_key",
                            "parameters_json",
                            "parameter_digest",
                            "ttl_ms",
                            "component",
                            "release_id",
                            "machine_id",
                            "boot_id",
                            "nonce_digest",
                            "authorization_digest",
                            "accepted_at_ms",
                            "status",
                            "rejection_reason",
                        )
                    ),
                    _intent_hash(intent_fields),
                ),
            )
            state = FaultEventState.AUTHORIZED if reason == "accepted" else FaultEventState.REJECTED
            self._append_event_in_transaction(
                con,
                request.fault_id,
                state,
                occurred_at_ms=accepted_at_ms,
                evidence={"reason": reason},
            )
            con.execute("COMMIT")
            return IntentAdmission(request.fault_id, reason == "accepted", reason)
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def claim_pending(
        self,
        identity: FaultRuntimeIdentity,
        *,
        claimed_at_ms: int,
    ) -> FaultIntent | None:
        if self._read_only:
            return None
        if not isinstance(identity, FaultRuntimeIdentity):
            return None
        self._validate_time(claimed_at_ms, "invalid-claimed-at")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            current = con.execute(
                "SELECT * "
                "FROM neg_risk_fault_runtime_starts WHERE component=? "
                "ORDER BY started_at_ms DESC,id DESC LIMIT 1",
                (identity.component,),
            ).fetchone()
            if (
                current is None
                or not self._runtime_row_valid(current)
                or self._runtime_from_row(current) != identity
            ):
                con.execute("COMMIT")
                return None
            rows = con.execute(
                "SELECT i.* FROM neg_risk_fault_intents i "
                "WHERE i.status='accepted' AND i.component=? AND i.release_id=? "
                "AND i.machine_id=? AND i.boot_id=? "
                "AND i.accepted_at_ms+i.ttl_ms>? "
                "ORDER BY i.accepted_at_ms,i.fault_id",
                (
                    identity.component,
                    identity.release_id,
                    identity.machine_id,
                    str(identity.boot_id),
                    claimed_at_ms,
                ),
            ).fetchall()
            row = None
            for candidate in rows:
                history = self._validate_history_in_connection(con, candidate["fault_id"])
                if (
                    history.valid
                    and history.events
                    and history.events[-1].state is FaultEventState.AUTHORIZED
                ):
                    row = candidate
                    break
            if row is None:
                con.execute("COMMIT")
                return None
            capability = FaultOwnershipCapability(
                fault_id=row["fault_id"],
                runtime=identity,
                token=secrets.token_hex(32),
            )
            self._append_event_in_transaction(
                con,
                row["fault_id"],
                FaultEventState.ARMED,
                occurred_at_ms=claimed_at_ms,
                evidence={
                    "runtime_identity_digest": canonical_digest(
                        {
                            "boot_id": str(identity.boot_id),
                            "component": identity.component,
                            "machine_id": identity.machine_id,
                            "release_id": identity.release_id,
                        }
                    ),
                    "ownership_digest": _ownership_digest(capability),
                },
            )
            con.execute("COMMIT")
            return replace(
                self._intent_from_row(row),
                ownership_capability=capability,
            )
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _append_event_in_transaction(
        self,
        con: sqlite3.Connection,
        fault_id: str,
        state: FaultEventState,
        *,
        occurred_at_ms: int,
        evidence: Mapping[str, object],
        action: str | None = None,
    ) -> FaultEvent:
        latest = con.execute(
            "SELECT sequence,event_hash FROM neg_risk_fault_events "
            "WHERE fault_id=? ORDER BY sequence DESC LIMIT 1",
            (fault_id,),
        ).fetchone()
        sequence = 1 if latest is None else int(latest["sequence"]) + 1
        previous_hash = _ZERO_HASH if latest is None else str(latest["event_hash"])
        normalized_evidence = normalize_evidence(state, evidence)
        evidence_json = canonical_json(normalized_evidence)
        digest = _event_hash(
            fault_id=fault_id,
            sequence=sequence,
            state=state,
            action=action,
            occurred_at_ms=occurred_at_ms,
            evidence_json=evidence_json,
            previous_hash=previous_hash,
        )
        cursor = con.execute(
            "INSERT INTO neg_risk_fault_events("
            "fault_id,sequence,state,action,occurred_at_ms,evidence_json,"
            "previous_hash,event_hash) VALUES (?,?,?,?,?,?,?,?)",
            (
                fault_id,
                sequence,
                state.value,
                action,
                occurred_at_ms,
                evidence_json,
                previous_hash,
                digest,
            ),
        )
        return FaultEvent(
            event_id=int(cursor.lastrowid),
            fault_id=fault_id,
            sequence=sequence,
            state=state,
            action=action,
            occurred_at_ms=occurred_at_ms,
            evidence=json.loads(evidence_json),
            previous_hash=previous_hash,
            event_hash=digest,
        )

    def append_event(
        self,
        fault_id: str,
        state: FaultEventState,
        *,
        occurred_at_ms: int,
        evidence: Mapping[str, object],
        ownership: FaultOwnershipCapability | None = None,
    ) -> FaultEvent:
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        typed_state = FaultEventState(state)
        self._validate_time(occurred_at_ms, "invalid-occurred-at")
        if not isinstance(evidence, Mapping):
            raise ValueError("invalid-evidence")
        normalize_evidence(typed_state, evidence)
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            intent_row = con.execute(
                "SELECT * FROM neg_risk_fault_intents WHERE fault_id=?",
                (fault_id,),
            ).fetchone()
            if intent_row is None:
                raise ValueError("fault-not-found")
            if typed_state in _PROCESS_OWNED_STATES:
                self._require_ownership(
                    con,
                    intent_row,
                    ownership,
                )
            event = self._append_event_in_transaction(
                con,
                fault_id,
                typed_state,
                occurred_at_ms=occurred_at_ms,
                evidence=evidence,
            )
            con.execute("COMMIT")
            return event
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _require_ownership(
        self,
        con: sqlite3.Connection,
        intent_row: sqlite3.Row,
        ownership: FaultOwnershipCapability | None,
    ) -> None:
        if (
            not isinstance(ownership, FaultOwnershipCapability)
            or ownership.fault_id != intent_row["fault_id"]
        ):
            raise PermissionError("ownership-capability-required")
        intent = self._intent_from_row(intent_row)
        if ownership.runtime != intent.runtime:
            raise PermissionError("ownership-capability-required")
        armed = con.execute(
            "SELECT evidence_json FROM neg_risk_fault_events WHERE fault_id=? AND state='armed'",
            (intent.fault_id,),
        ).fetchone()
        if armed is None:
            raise PermissionError("ownership-capability-required")
        try:
            evidence = json.loads(armed["evidence_json"])
            matches = evidence["ownership_digest"] == _ownership_digest(ownership)
        except (KeyError, TypeError, ValueError, json.JSONDecodeError):
            matches = False
        if not matches:
            raise PermissionError("ownership-capability-required")

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> FaultEvent:
        return FaultEvent(
            event_id=row["id"],
            fault_id=row["fault_id"],
            sequence=row["sequence"],
            state=FaultEventState(row["state"]),
            action=row["action"],
            occurred_at_ms=row["occurred_at_ms"],
            evidence=json.loads(row["evidence_json"]),
            previous_hash=row["previous_hash"],
            event_hash=row["event_hash"],
        )

    @staticmethod
    def _nonce_row_valid(row: sqlite3.Row) -> bool:
        try:
            return row["row_hash"] == _nonce_hash(
                nonce_digest=row["nonce_digest"],
                authorization_digest=row["authorization_digest"],
                accepted_at_ms=row["accepted_at_ms"],
            )
        except (KeyError, TypeError, ValueError, IndexError):
            return False

    def _intent_row_valid(
        self,
        con: sqlite3.Connection,
        row: sqlite3.Row,
    ) -> bool:
        try:
            parameters = json.loads(row["parameters_json"])
            if canonical_json(parameters) != row["parameters_json"]:
                return False
            if (
                hashlib.sha256(row["parameters_json"].encode()).hexdigest()
                != row["parameter_digest"]
            ):
                return False
            if row["intent_hash"] != _intent_hash(row):
                return False
            self._intent_from_row(row)
            runtime = con.execute(
                "SELECT * FROM neg_risk_fault_runtime_starts "
                "WHERE component=? AND release_id=? AND machine_id=? AND boot_id=?",
                (
                    row["component"],
                    row["release_id"],
                    row["machine_id"],
                    row["boot_id"],
                ),
            ).fetchone()
            if runtime is None or not self._runtime_row_valid(runtime):
                return False
            nonce = con.execute(
                "SELECT * FROM neg_risk_fault_auth_nonces WHERE nonce_digest=?",
                (row["nonce_digest"],),
            ).fetchone()
            if nonce is None or not self._nonce_row_valid(nonce):
                return False
            return (
                row["status"] != "accepted"
                or nonce["authorization_digest"] == row["authorization_digest"]
            )
        except (
            KeyError,
            TypeError,
            ValueError,
            IndexError,
            json.JSONDecodeError,
            sqlite3.Error,
        ):
            return False

    def _validate_history_in_connection(
        self,
        con: sqlite3.Connection,
        fault_id: str,
    ) -> FaultHistory:
        row = con.execute(
            "SELECT * FROM neg_risk_fault_intents WHERE fault_id=?",
            (fault_id,),
        ).fetchone()
        if row is None:
            return FaultHistory(fault_id, False, "intent-missing", None, ())
        if not self._intent_row_valid(con, row):
            return FaultHistory(fault_id, False, "intent-integrity-invalid", None, ())
        try:
            intent = self._intent_from_row(row)
            event_rows = con.execute(
                "SELECT * FROM neg_risk_fault_events WHERE fault_id=? ORDER BY sequence,id",
                (fault_id,),
            ).fetchall()
            events = tuple(self._event_from_row(value) for value in event_rows)
        except (sqlite3.Error, ValueError, TypeError, json.JSONDecodeError):
            return FaultHistory(fault_id, False, "authority-unavailable", None, ())

        previous = _ZERO_HASH
        previous_state: FaultEventState | None = None
        previous_time = -1
        expected_first = (
            FaultEventState.AUTHORIZED if row["status"] == "accepted" else FaultEventState.REJECTED
        )
        for index, event in enumerate(events, start=1):
            if event.sequence != index or event.previous_hash != previous:
                return FaultHistory(fault_id, False, "hash-predecessor-invalid", intent, events)
            try:
                evidence_json = canonical_json(normalize_evidence(event.state, event.evidence))
                expected_hash = _event_hash(
                    fault_id=event.fault_id,
                    sequence=event.sequence,
                    state=event.state,
                    action=event.action,
                    occurred_at_ms=event.occurred_at_ms,
                    evidence_json=evidence_json,
                    previous_hash=event.previous_hash,
                )
            except (ValueError, TypeError):
                return FaultHistory(
                    fault_id, False, "event-canonicalization-invalid", intent, events
                )
            if event.event_hash != expected_hash:
                return FaultHistory(fault_id, False, "event-hash-invalid", intent, events)
            if event.occurred_at_ms < previous_time:
                return FaultHistory(fault_id, False, "event-time-regression", intent, events)
            if previous_state is None:
                if event.state is not expected_first:
                    return FaultHistory(fault_id, False, "lifecycle-origin-invalid", intent, events)
            elif event.state not in _NEXT_STATES.get(previous_state, frozenset()):
                return FaultHistory(fault_id, False, "lifecycle-regression", intent, events)
            previous = event.event_hash
            previous_state = event.state
            previous_time = event.occurred_at_ms
        if not events:
            return FaultHistory(fault_id, False, "event-history-missing", intent, events)
        return FaultHistory(fault_id, True, "valid", intent, events)

    def validate_history(self, fault_id: str) -> FaultHistory:
        try:
            con = self._connect()
            try:
                return self._validate_history_in_connection(con, fault_id)
            finally:
                con.close()
        except (sqlite3.Error, ValueError, TypeError, json.JSONDecodeError):
            return FaultHistory(fault_id, False, "authority-unavailable", None, ())

    def project_fault(self, fault_id: str, *, now_ms: int) -> FaultProjection:
        try:
            self._validate_time(now_ms, "invalid-now")
            history = self.validate_history(fault_id)
            if not history.valid or history.intent is None:
                return FaultProjection(fault_id, False, False, None, history.reason, history.intent)
            con = self._connect()
            try:
                accepted_rows = con.execute(
                    "SELECT fault_id FROM neg_risk_fault_intents WHERE status='accepted'"
                ).fetchall()
            finally:
                con.close()
            active_count = 0
            for accepted in accepted_rows:
                candidate = self.validate_history(accepted["fault_id"])
                if (
                    candidate.valid
                    and candidate.intent is not None
                    and candidate.events
                    and candidate.events[-1].state not in _TERMINAL_STATES
                    and self.current_runtime(candidate.intent.runtime.component)
                    == candidate.intent.runtime
                ):
                    active_count += 1
            if active_count > 1:
                return FaultProjection(
                    fault_id, False, False, None, "multiple-active-chains", history.intent
                )
            current = self.current_runtime(history.intent.runtime.component)
            latest = history.events[-1].state
            if current != history.intent.runtime:
                return FaultProjection(
                    fault_id,
                    True,
                    False,
                    FaultEventState.ABANDONED,
                    "runtime-replaced",
                    history.intent,
                )
            if (
                latest in {FaultEventState.AUTHORIZED, FaultEventState.ARMED}
                and now_ms >= history.intent.accepted_at_ms + history.intent.ttl_ms
            ):
                return FaultProjection(
                    fault_id,
                    True,
                    False,
                    FaultEventState.EXPIRED,
                    "intent-expired",
                    history.intent,
                )
            return FaultProjection(
                fault_id,
                True,
                latest not in _TERMINAL_STATES,
                latest,
                "valid",
                history.intent,
            )
        except (sqlite3.Error, ValueError, TypeError):
            return FaultProjection(fault_id, False, False, None, "authority-unavailable", None)
