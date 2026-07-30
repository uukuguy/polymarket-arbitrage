"""Append-only SQLite authority for scoped upstream fault control."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import sqlite3
import time
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from uuid import UUID

from polyarb.perception.fault_control import (
    FaultAuthoritySnapshot,
    FaultAuthorization,
    FaultCallClass,
    FaultEvent,
    FaultEventAction,
    FaultEventState,
    FaultHistory,
    FaultIntent,
    FaultIntentRequest,
    FaultKind,
    FaultOwnershipCapability,
    FaultProjection,
    FaultRecoveryReceipt,
    FaultRecoveryWriter,
    FaultRuntimeIdentity,
    IntentAdmission,
    canonical_digest,
    canonical_json,
    normalize_evidence,
    normalize_fault_id,
    normalize_supervisor_run_id,
)
from polyarb.perception.store import (
    candidate_success_receipt_hash,
    validate_reconciliation_authority_checkpoint,
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
            FaultEventState.ABANDONED,
            FaultEventState.CLEANUP_FAILED,
            FaultEventState.EVIDENCE_INVALID,
            FaultEventState.ESCALATED,
        }
    ),
    FaultEventState.DETECTED: frozenset(
        {
            FaultEventState.CONTAINED,
            FaultEventState.ABANDONED,
            FaultEventState.CLEANUP_FAILED,
            FaultEventState.EVIDENCE_INVALID,
            FaultEventState.ESCALATED,
        }
    ),
    FaultEventState.CONTAINED: frozenset(
        {
            FaultEventState.CLEANED,
            FaultEventState.CLEANUP_FAILED,
            FaultEventState.EVIDENCE_INVALID,
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
        FaultEventState.EVIDENCE_INVALID,
    }
)
_FAULT_COMPONENTS = ("candidate", "discovery", "reconciliation", "notification")


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
    record_type: str,
    nonce_digest: str,
    authorization_digest: str,
    operation: str,
    fault_id: str | None,
    request_digest: str,
    outcome: str | None,
    reason: str | None,
    occurred_at_ms: int,
    reservation_id: int | None,
) -> str:
    return canonical_digest(
        {
            "authorization_digest": authorization_digest,
            "fault_id": fault_id,
            "nonce_digest": nonce_digest,
            "occurred_at_ms": occurred_at_ms,
            "operation": operation,
            "outcome": outcome,
            "reason": reason,
            "record_type": record_type,
            "request_digest": request_digest,
            "reservation_id": reservation_id,
        }
    )


def _intent_hash(fields: Mapping[str, object]) -> str:
    return canonical_digest(
        {
            "accepted_at_ms": fields["accepted_at_ms"],
            "authorization_digest": fields["authorization_digest"],
            "auth_attempt_id": fields["auth_attempt_id"],
            "auth_reservation_id": fields["auth_reservation_id"],
            "boot_id": fields["boot_id"],
            "call_class": fields["call_class"],
            "component": fields["component"],
            "fault_id": fields["fault_id"],
            "kind": fields["kind"],
            "machine_id": fields["machine_id"],
            "nonce_digest": fields["nonce_digest"],
            "parameter_digest": fields["parameter_digest"],
            "parameters_json": fields["parameters_json"],
            "request_digest": fields["request_digest"],
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
    state: FaultEventState | None,
    action: FaultEventAction | None,
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
            "state": state.value if state is not None else None,
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

    @staticmethod
    def _check_deadline(deadline_monotonic: float | None) -> None:
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            raise TimeoutError("fault-authority-deadline")

    @staticmethod
    def _normalize_deadline_error(error: BaseException, deadline_monotonic: float | None) -> None:
        if (
            deadline_monotonic is not None
            and isinstance(error, sqlite3.OperationalError)
            and any(marker in str(error).lower() for marker in ("locked", "interrupted"))
        ):
            raise TimeoutError("fault-authority-deadline") from error

    @staticmethod
    def _clear_progress_handler(con: sqlite3.Connection) -> None:
        con.set_progress_handler(None, 0)

    @classmethod
    def _commit_before_deadline(
        cls,
        con: sqlite3.Connection,
        deadline_monotonic: float | None,
    ) -> None:
        cls._check_deadline(deadline_monotonic)
        if deadline_monotonic is not None:
            remaining_ms = max(1, int(max(0.001, deadline_monotonic - time.monotonic()) * 1_000))
            con.execute(f"PRAGMA busy_timeout={remaining_ms}")
            cls._check_deadline(deadline_monotonic)
        con.execute("COMMIT")

    def _connect(self, deadline_monotonic: float | None = None) -> sqlite3.Connection:
        self._check_deadline(deadline_monotonic)
        timeout_ms = self._busy_timeout_ms
        if deadline_monotonic is not None:
            timeout_ms = max(
                1,
                min(
                    timeout_ms,
                    int(max(0.001, deadline_monotonic - time.monotonic()) * 1_000),
                ),
            )
        target = (
            f"file:{self._db_path.resolve()}?mode=ro" if self._read_only else str(self._db_path)
        )
        con = sqlite3.connect(
            target,
            uri=self._read_only,
            isolation_level=None,
            timeout=timeout_ms / 1_000,
        )
        con.row_factory = sqlite3.Row
        con.execute(f"PRAGMA busy_timeout={timeout_ms}")
        con.execute("PRAGMA foreign_keys=ON")
        if deadline_monotonic is not None:
            con.set_progress_handler(
                lambda: int(time.monotonic() >= deadline_monotonic),
                1_000,
            )
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
            supervisor_run_id = normalize_supervisor_run_id(supervisor_run_id)
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

    def record_partial_coverage_rejection(
        self,
        fault_id: str,
        *,
        coverage_id: str,
        original_count: int,
        kept_count: int,
        requested_cursor_digest: str,
        next_cursor_digest: str,
        recorded_at_ms: int,
    ) -> None:
        """Persist the producer-owned Gamma partial-page fact before detection."""
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        fault_id = normalize_fault_id(fault_id)
        self._validate_time(recorded_at_ms, "invalid-coverage-recorded-at")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT * FROM neg_risk_fault_intents WHERE fault_id=?",
                (fault_id,),
            ).fetchone()
            if (
                row is None
                or row["kind"] != "gamma-partial"
                or row["call_class"] != "gamma-discovery-event-page"
                or row["target_key"] != "discovery"
                or type(original_count) is not int
                or type(kept_count) is not int
                or not 0 <= kept_count < original_count
                or not isinstance(coverage_id, str)
                or not coverage_id.startswith("coverage-")
                or len(coverage_id) != 73
                or any(
                    not isinstance(value, str)
                    or re.fullmatch(r"[0-9a-f]{64}", value) is None
                    for value in (requested_cursor_digest, next_cursor_digest)
                )
            ):
                raise ValueError("invalid-partial-coverage-source")
            payload = {
                "boot_id": row["boot_id"],
                "call_class": row["call_class"],
                "component": row["component"],
                "coverage_id": coverage_id,
                "fault_id": fault_id,
                "kept_count": kept_count,
                "machine_id": row["machine_id"],
                "next_cursor_digest": next_cursor_digest,
                "original_count": original_count,
                "recorded_at_ms": recorded_at_ms,
                "release_id": row["release_id"],
                "requested_cursor_digest": requested_cursor_digest,
                "target_key": row["target_key"],
            }
            con.execute(
                "INSERT INTO neg_risk_fault_coverage_rejections("
                + ",".join(payload)
                + ",source_hash) VALUES("
                + ",".join("?" for _ in range(len(payload) + 1))
                + ")",
                (*payload.values(), canonical_digest(payload)),
            )
            con.execute("COMMIT")
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
                return self._current_runtime_in_connection(con, component)
            finally:
                con.close()
        except sqlite3.Error:
            return None

    def _current_runtime_in_connection(
        self, con: sqlite3.Connection, component: str
    ) -> FaultRuntimeIdentity | None:
        row = con.execute(
            "SELECT * FROM neg_risk_fault_runtime_starts WHERE component=? "
            "ORDER BY started_at_ms DESC,id DESC LIMIT 1",
            (component,),
        ).fetchone()
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
            "AND state IS NOT NULL ORDER BY sequence DESC LIMIT 1",
            (fault_id,),
        ).fetchone()
        return FaultEventState(row["state"]) if row is not None else None

    def _current_active_fault_ids(
        self,
        con: sqlite3.Connection,
        *,
        deadline_monotonic: float | None,
        limit: int,
    ) -> tuple[str, ...]:
        self._check_deadline(deadline_monotonic)
        active: list[str] = []
        terminal_values = tuple(state.value for state in _TERMINAL_STATES)
        terminal_placeholders = ",".join("?" for _ in terminal_values)
        for component in _FAULT_COMPONENTS:
            self._check_deadline(deadline_monotonic)
            runtime = self._current_runtime_in_connection(con, component)
            if runtime is None:
                continue
            rows = con.execute(
                "SELECT i.fault_id FROM neg_risk_fault_intents i "
                "WHERE i.component=? AND i.release_id=? "
                "AND i.machine_id=? AND i.boot_id=? "
                "AND i.status='accepted' "
                "AND COALESCE(("
                " SELECT e.state FROM neg_risk_fault_events e "
                " WHERE e.fault_id=i.fault_id AND e.state IS NOT NULL "
                " ORDER BY e.sequence DESC LIMIT 1"
                "), '') NOT IN (" + terminal_placeholders + ") "
                "ORDER BY i.accepted_at_ms DESC,i.fault_id DESC LIMIT 2",
                (
                    runtime.component,
                    runtime.release_id,
                    runtime.machine_id,
                    str(runtime.boot_id),
                    *terminal_values,
                ),
            ).fetchall()
            self._check_deadline(deadline_monotonic)
            for row in rows:
                self._check_deadline(deadline_monotonic)
                active.append(str(row["fault_id"]))
                if len(active) >= limit:
                    break
            if len(active) >= limit:
                break
        self._check_deadline(deadline_monotonic)
        return tuple(active)

    def _has_active_chain(
        self,
        con: sqlite3.Connection,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        return bool(
            self._current_active_fault_ids(con, deadline_monotonic=deadline_monotonic, limit=1)
        )

    @staticmethod
    def _auth_row_fields(
        *,
        record_type: str,
        auth: FaultAuthorization,
        operation: str,
        fault_id: str | None,
        request_digest: str,
        outcome: str | None,
        reason: str | None,
        occurred_at_ms: int,
        reservation_id: int | None,
    ) -> dict[str, object]:
        return {
            "record_type": record_type,
            "nonce_digest": auth.nonce_digest,
            "authorization_digest": auth.authorization_digest,
            "operation": operation,
            "fault_id": fault_id,
            "request_digest": request_digest,
            "outcome": outcome,
            "reason": reason,
            "occurred_at_ms": occurred_at_ms,
            "reservation_id": reservation_id,
        }

    def _insert_auth_row(
        self,
        con: sqlite3.Connection,
        fields: Mapping[str, object],
    ) -> int:
        digest = _nonce_hash(**fields)  # type: ignore[arg-type]
        cursor = con.execute(
            "INSERT INTO neg_risk_fault_auth_nonces("
            "record_type,nonce_digest,authorization_digest,operation,fault_id,"
            "request_digest,outcome,reason,occurred_at_ms,reservation_id,row_hash)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
            (
                *(
                    fields[key]
                    for key in (
                        "record_type",
                        "nonce_digest",
                        "authorization_digest",
                        "operation",
                        "fault_id",
                        "request_digest",
                        "outcome",
                        "reason",
                        "occurred_at_ms",
                        "reservation_id",
                    )
                ),
                digest,
            ),
        )
        return int(cursor.lastrowid or 0)

    def _reserve_auth(
        self,
        con: sqlite3.Connection,
        *,
        auth: FaultAuthorization,
        operation: str,
        fault_id: str | None,
        request_digest: str,
        occurred_at_ms: int,
    ) -> tuple[int, bool]:
        existing = con.execute(
            "SELECT * FROM neg_risk_fault_auth_nonces "
            "WHERE record_type='reservation' AND nonce_digest=?",
            (auth.nonce_digest,),
        ).fetchone()
        if existing is not None:
            if not self._nonce_row_valid(existing):
                raise ValueError("fault-auth-history-invalid")
            return int(existing["id"]), True
        fields = self._auth_row_fields(
            record_type="reservation",
            auth=auth,
            operation=operation,
            fault_id=fault_id,
            request_digest=request_digest,
            outcome=None,
            reason=None,
            occurred_at_ms=occurred_at_ms,
            reservation_id=None,
        )
        return self._insert_auth_row(con, fields), False

    def _append_auth_attempt(
        self,
        con: sqlite3.Connection,
        *,
        auth: FaultAuthorization,
        operation: str,
        fault_id: str | None,
        request_digest: str,
        outcome: str,
        reason: str,
        occurred_at_ms: int,
        reservation_id: int,
    ) -> int:
        return self._insert_auth_row(
            con,
            self._auth_row_fields(
                record_type="attempt",
                auth=auth,
                operation=operation,
                fault_id=fault_id,
                request_digest=request_digest,
                outcome=outcome,
                reason=reason,
                occurred_at_ms=occurred_at_ms,
                reservation_id=reservation_id,
            ),
        )

    def reject_control_attempt(
        self,
        *,
        auth: FaultAuthorization,
        operation: str,
        fault_id: str | None,
        request_digest: str,
        reason: str,
        occurred_at_ms: int,
        deadline_monotonic: float | None = None,
    ) -> str:
        """Audit an authenticated rejection without fabricating an intent."""
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        self._validate_time(occurred_at_ms, "invalid-occurred-at")
        con = self._connect(deadline_monotonic)
        try:
            con.execute("BEGIN IMMEDIATE")
            self._check_deadline(deadline_monotonic)
            reservation_id, replay = self._reserve_auth(
                con,
                auth=auth,
                operation=operation,
                fault_id=fault_id,
                request_digest=request_digest,
                occurred_at_ms=occurred_at_ms,
            )
            final_reason = "nonce-replay" if replay else reason
            self._check_deadline(deadline_monotonic)
            self._append_auth_attempt(
                con,
                auth=auth,
                operation=operation,
                fault_id=fault_id,
                request_digest=request_digest,
                outcome="rejected",
                reason=final_reason,
                occurred_at_ms=occurred_at_ms,
                reservation_id=reservation_id,
            )
            self._check_deadline(deadline_monotonic)
            self._commit_before_deadline(con, deadline_monotonic)
            return final_reason
        except BaseException as error:
            self._clear_progress_handler(con)
            if con.in_transaction:
                con.execute("ROLLBACK")
            self._normalize_deadline_error(error, deadline_monotonic)
            raise
        finally:
            self._clear_progress_handler(con)
            con.close()

    def accept_intent(
        self,
        request: FaultIntentRequest,
        *,
        auth: FaultAuthorization,
        accepted_at_ms: int,
        request_digest: str | None = None,
        deadline_monotonic: float | None = None,
    ) -> IntentAdmission:
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        if not isinstance(request, FaultIntentRequest) or not isinstance(auth, FaultAuthorization):
            raise ValueError("invalid-intent-envelope")
        self._validate_time(accepted_at_ms, "invalid-accepted-at")
        request_digest = request_digest or canonical_digest(
            {
                "fault_id": request.fault_id,
                "kind": request.kind.value,
                "runtime": str(request.runtime.boot_id),
            }
        )
        con = self._connect(deadline_monotonic)
        try:
            con.execute("BEGIN IMMEDIATE")
            self._check_deadline(deadline_monotonic)
            reservation_id, replay = self._reserve_auth(
                con,
                auth=auth,
                operation="arm",
                fault_id=request.fault_id,
                request_digest=request_digest,
                occurred_at_ms=accepted_at_ms,
            )
            current = con.execute(
                "SELECT * "
                "FROM neg_risk_fault_runtime_starts WHERE component=? "
                "ORDER BY started_at_ms DESC,id DESC LIMIT 1",
                (request.runtime.component,),
            ).fetchone()
            reason = "accepted"
            if replay:
                reason = "nonce-replay"
            elif current is None or not self._runtime_row_valid(current):
                reason = "runtime-unavailable"
            elif self._runtime_from_row(current) != request.runtime:
                reason = "runtime-mismatch"
            elif self._has_active_chain(con, deadline_monotonic=deadline_monotonic):
                reason = "fault-already-active"

            self._check_deadline(deadline_monotonic)
            attempt_id = self._append_auth_attempt(
                con,
                auth=auth,
                operation="arm",
                fault_id=request.fault_id,
                request_digest=request_digest,
                outcome="accepted" if reason == "accepted" else "rejected",
                reason=reason,
                occurred_at_ms=accepted_at_ms,
                reservation_id=reservation_id,
            )
            if reason != "accepted":
                self._commit_before_deadline(con, deadline_monotonic)
                return IntentAdmission(request.fault_id, False, reason)
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
                "request_digest": request_digest,
                "auth_reservation_id": reservation_id,
                "auth_attempt_id": attempt_id,
                "accepted_at_ms": accepted_at_ms,
                "status": "accepted",
                "rejection_reason": None,
            }
            self._check_deadline(deadline_monotonic)
            con.execute(
                "INSERT INTO neg_risk_fault_intents("
                "fault_id,kind,call_class,target_key,parameters_json,parameter_digest,"
                "ttl_ms,component,release_id,machine_id,boot_id,nonce_digest,"
                "authorization_digest,request_digest,auth_reservation_id,"
                "auth_attempt_id,accepted_at_ms,status,rejection_reason,intent_hash)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
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
                            "request_digest",
                            "auth_reservation_id",
                            "auth_attempt_id",
                            "accepted_at_ms",
                            "status",
                            "rejection_reason",
                        )
                    ),
                    _intent_hash(intent_fields),
                ),
            )
            self._check_deadline(deadline_monotonic)
            self._append_event_in_transaction(
                con,
                request.fault_id,
                FaultEventState.AUTHORIZED,
                occurred_at_ms=accepted_at_ms,
                evidence={"reason": "accepted"},
            )
            self._commit_before_deadline(con, deadline_monotonic)
            return IntentAdmission(request.fault_id, True, "accepted")
        except BaseException as error:
            self._clear_progress_handler(con)
            if con.in_transaction:
                con.execute("ROLLBACK")
            self._normalize_deadline_error(error, deadline_monotonic)
            raise
        finally:
            self._clear_progress_handler(con)
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
                latest_lifecycle = next(
                    (event.state for event in reversed(history.events) if event.state is not None),
                    None,
                )
                if (
                    history.valid
                    and history.events
                    and latest_lifecycle is FaultEventState.AUTHORIZED
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
        state: FaultEventState | None,
        *,
        occurred_at_ms: int,
        evidence: Mapping[str, object],
        action: FaultEventAction | None = None,
    ) -> FaultEvent:
        latest = con.execute(
            "SELECT sequence,event_hash FROM neg_risk_fault_events "
            "WHERE fault_id=? ORDER BY sequence DESC LIMIT 1",
            (fault_id,),
        ).fetchone()
        sequence = 1 if latest is None else int(latest["sequence"]) + 1
        previous_hash = _ZERO_HASH if latest is None else str(latest["event_hash"])
        if state is None:
            request_fields = {
                "authorization_digest", "nonce_digest", "request_digest",
                "reservation_id", "attempt_id",
            }
            confirmation_fields = {
                "cleaned_event_hash", "cleanup_id", "memory_cleared_at_ms",
                "receipt_commit_confirmed_at_ms",
            }
            expected_fields = (
                request_fields
                if action is FaultEventAction.CLEANUP_REQUESTED
                else confirmation_fields
                if action is FaultEventAction.CLEANUP_CONFIRMED
                else set()
            )
            if set(evidence) != expected_fields:
                raise ValueError("invalid-action-evidence")
            if action is FaultEventAction.CLEANUP_CONFIRMED:
                if (
                    re.fullmatch(r"[0-9a-f]{64}", str(evidence["cleaned_event_hash"]))
                    is None
                    or re.fullmatch(r"[a-zA-Z0-9._:-]{1,128}", str(evidence["cleanup_id"]))
                    is None
                    or any(
                        type(evidence[key]) is not int or int(evidence[key]) < 0
                        for key in (
                            "memory_cleared_at_ms",
                            "receipt_commit_confirmed_at_ms",
                        )
                    )
                    or int(evidence["receipt_commit_confirmed_at_ms"])
                    < int(evidence["memory_cleared_at_ms"])
                ):
                    raise ValueError("invalid-action-evidence")
                normalized_evidence = dict(evidence)
            else:
                for key in ("authorization_digest", "nonce_digest", "request_digest"):
                    value = evidence[key]
                    if (
                        not isinstance(value, str)
                        or len(value) != 64
                        or not set(value) <= set("0123456789abcdef")
                    ):
                        raise ValueError("invalid-action-evidence")
                for key in ("reservation_id", "attempt_id"):
                    value = evidence[key]
                    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                        raise ValueError("invalid-action-evidence")
                normalized_evidence = dict(evidence)
        else:
            if action is not None:
                raise ValueError("invalid-event-action")
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
                state.value if state is not None else None,
                action.value if action is not None else None,
                occurred_at_ms,
                evidence_json,
                previous_hash,
                digest,
            ),
        )
        return FaultEvent(
            event_id=int(cursor.lastrowid or 0),
            fault_id=fault_id,
            sequence=sequence,
            state=state,
            action=action,
            occurred_at_ms=occurred_at_ms,
            evidence=json.loads(evidence_json),
            previous_hash=previous_hash,
            event_hash=digest,
        )

    def request_cleanup(
        self,
        fault_id: str,
        *,
        auth: FaultAuthorization,
        requested_at_ms: int,
        request_digest: str | None = None,
        deadline_monotonic: float | None = None,
    ) -> FaultEvent:
        """Append a request action; only the owning process can prove cleanup."""
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        if not isinstance(auth, FaultAuthorization):
            raise ValueError("invalid-intent-envelope")
        fault_id = normalize_fault_id(fault_id)
        self._validate_time(requested_at_ms, "invalid-requested-at")
        request_digest = request_digest or canonical_digest(
            {"fault_id": fault_id, "operation": "cleanup"}
        )
        con = self._connect(deadline_monotonic)
        try:
            con.execute("BEGIN IMMEDIATE")
            self._check_deadline(deadline_monotonic)
            reservation_id, replay = self._reserve_auth(
                con,
                auth=auth,
                operation="cleanup",
                fault_id=fault_id,
                request_digest=request_digest,
                occurred_at_ms=requested_at_ms,
            )
            existing = con.execute(
                "SELECT * FROM neg_risk_fault_events WHERE fault_id=? "
                "AND action='cleanup-requested' ORDER BY sequence LIMIT 1",
                (fault_id,),
            ).fetchone()
            if replay:
                self._check_deadline(deadline_monotonic)
                self._append_auth_attempt(
                    con,
                    auth=auth,
                    operation="cleanup",
                    fault_id=fault_id,
                    request_digest=request_digest,
                    outcome="rejected",
                    reason="nonce-replay",
                    occurred_at_ms=requested_at_ms,
                    reservation_id=reservation_id,
                )
                self._check_deadline(deadline_monotonic)
                self._commit_before_deadline(con, deadline_monotonic)
                raise ValueError("nonce-replay")
            intent = con.execute(
                "SELECT 1 FROM neg_risk_fault_intents WHERE fault_id=? AND status='accepted'",
                (fault_id,),
            ).fetchone()
            if intent is None:
                reason = "fault-not-found"
                self._check_deadline(deadline_monotonic)
                self._append_auth_attempt(
                    con,
                    auth=auth,
                    operation="cleanup",
                    fault_id=fault_id,
                    request_digest=request_digest,
                    outcome="rejected",
                    reason=reason,
                    occurred_at_ms=requested_at_ms,
                    reservation_id=reservation_id,
                )
                self._check_deadline(deadline_monotonic)
                self._commit_before_deadline(con, deadline_monotonic)
                raise ValueError(reason)
            if existing is not None:
                history = self._validate_history_in_connection(
                    con,
                    fault_id,
                    deadline_monotonic=deadline_monotonic,
                )
                if not history.valid:
                    raise ValueError("fault-history-invalid")
                self._check_deadline(deadline_monotonic)
                attempt_id = self._append_auth_attempt(
                    con,
                    auth=auth,
                    operation="cleanup",
                    fault_id=fault_id,
                    request_digest=request_digest,
                    outcome="accepted",
                    reason="cleanup-already-requested",
                    occurred_at_ms=requested_at_ms,
                    reservation_id=reservation_id,
                )
                self._check_deadline(deadline_monotonic)
                self._commit_before_deadline(con, deadline_monotonic)
                return self._event_from_row(existing)
            self._check_deadline(deadline_monotonic)
            attempt_id = self._append_auth_attempt(
                con,
                auth=auth,
                operation="cleanup",
                fault_id=fault_id,
                request_digest=request_digest,
                outcome="accepted",
                reason="cleanup-requested",
                occurred_at_ms=requested_at_ms,
                reservation_id=reservation_id,
            )
            evidence = {
                "authorization_digest": auth.authorization_digest,
                "nonce_digest": auth.nonce_digest,
                "request_digest": request_digest,
                "reservation_id": reservation_id,
                "attempt_id": attempt_id,
            }
            self._check_deadline(deadline_monotonic)
            event = self._append_event_in_transaction(
                con,
                fault_id,
                None,
                action=FaultEventAction.CLEANUP_REQUESTED,
                occurred_at_ms=requested_at_ms,
                evidence=evidence,
            )
            self._commit_before_deadline(con, deadline_monotonic)
            return event
        except BaseException as error:
            self._clear_progress_handler(con)
            if con.in_transaction:
                con.execute("ROLLBACK")
            self._normalize_deadline_error(error, deadline_monotonic)
            raise
        finally:
            self._clear_progress_handler(con)
            con.close()

    def confirm_cleanup_commit(
        self,
        fault_id: str,
        *,
        cleaned: FaultEvent,
        memory_cleared_at_ms: int,
        confirmed_at_ms: int,
        ownership: FaultOwnershipCapability,
    ) -> FaultEvent:
        """Append exact post-commit proof after the CLEANED transaction returned."""
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            intent_row = con.execute(
                "SELECT * FROM neg_risk_fault_intents WHERE fault_id=?",
                (fault_id,),
            ).fetchone()
            if intent_row is None:
                raise ValueError("fault-not-found")
            self._require_ownership(con, intent_row, ownership)
            latest = con.execute(
                "SELECT * FROM neg_risk_fault_events WHERE fault_id=? "
                "ORDER BY sequence DESC LIMIT 1",
                (fault_id,),
            ).fetchone()
            if latest is None or latest["event_hash"] != cleaned.event_hash:
                raise ValueError("cleanup-event-not-tail")
            cleanup_id = cleaned.evidence.get("cleanup_id")
            if cleaned.state is not FaultEventState.CLEANED or not isinstance(cleanup_id, str):
                raise ValueError("cleanup-event-invalid")
            event = self._append_event_in_transaction(
                con,
                fault_id,
                None,
                action=FaultEventAction.CLEANUP_CONFIRMED,
                occurred_at_ms=confirmed_at_ms,
                evidence={
                    "cleaned_event_hash": cleaned.event_hash,
                    "cleanup_id": cleanup_id,
                    "memory_cleared_at_ms": memory_cleared_at_ms,
                    "receipt_commit_confirmed_at_ms": confirmed_at_ms,
                },
            )
            con.execute("COMMIT")
            return event
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

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
            latest_state = self._latest_state(con, fault_id)
            process_owned_terminal = (
                typed_state in {FaultEventState.ABANDONED, FaultEventState.EXPIRED}
                and latest_state is not FaultEventState.AUTHORIZED
            )
            if typed_state in _PROCESS_OWNED_STATES or process_owned_terminal:
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

    def finalize_verdict(
        self,
        fault_id: str,
        *,
        verdict_id: str,
        verdict_digest: str,
        source_tail_hash: str,
        runtime: FaultRuntimeIdentity,
        auth: FaultAuthorization,
        request_digest: str,
        occurred_at_ms: int,
        deadline_monotonic: float | None = None,
    ) -> FaultEvent:
        """Append VERIFIED only for an exact signed RECOVERED source chain."""
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        fault_id = normalize_fault_id(fault_id)
        self._validate_time(occurred_at_ms, "invalid-occurred-at")
        normalize_evidence(
            FaultEventState.VERIFIED,
            {"verdict_id": verdict_id, "verdict_digest": verdict_digest},
        )
        con = self._connect(deadline_monotonic)
        try:
            con.execute("BEGIN IMMEDIATE")
            reservation_id, replay = self._reserve_auth(
                con,
                auth=auth,
                operation="finalize",
                fault_id=fault_id,
                request_digest=request_digest,
                occurred_at_ms=occurred_at_ms,
            )
            if replay:
                self._append_auth_attempt(
                    con, auth=auth, operation="finalize", fault_id=fault_id,
                    request_digest=request_digest, outcome="rejected",
                    reason="nonce-replay", occurred_at_ms=occurred_at_ms,
                    reservation_id=reservation_id,
                )
                con.execute("COMMIT")
                raise ValueError("nonce-replay")
            history = self._validate_history_in_connection(con, fault_id)
            if not history.valid or history.intent is None or not history.events:
                raise ValueError("fault-history-invalid")
            tail = history.events[-1]
            expected_evidence = {
                "verdict_id": verdict_id,
                "verdict_digest": verdict_digest,
            }
            if tail.state is FaultEventState.VERIFIED:
                if dict(tail.evidence) != expected_evidence:
                    raise ValueError("verdict-conflict")
                reason = "verdict-already-finalized"
                event = tail
            else:
                current_runtime = self._current_runtime_in_connection(
                    con, history.intent.runtime.component
                )
                if (
                    tail.state is not FaultEventState.RECOVERED
                    or tail.event_hash != source_tail_hash
                    or history.intent.runtime != runtime
                    or current_runtime != runtime
                ):
                    raise ValueError("verdict-source-mismatch")
                event = self._append_event_in_transaction(
                    con,
                    fault_id,
                    FaultEventState.VERIFIED,
                    occurred_at_ms=occurred_at_ms,
                    evidence=expected_evidence,
                )
                reason = "verdict-finalized"
            self._append_auth_attempt(
                con, auth=auth, operation="finalize", fault_id=fault_id,
                request_digest=request_digest, outcome="accepted", reason=reason,
                occurred_at_ms=occurred_at_ms, reservation_id=reservation_id,
            )
            con.execute("COMMIT")
            return event
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def append_recovery_event(
        self,
        receipt: FaultRecoveryReceipt,
        *,
        injected_at_ms: int,
        occurred_at_ms: int,
        ownership: FaultOwnershipCapability | None,
    ) -> FaultEvent | None:
        """Append recovery only when the exact post-injection writer row exists."""
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        if not isinstance(receipt, FaultRecoveryReceipt):
            return None
        self._validate_time(injected_at_ms, "invalid-injected-at")
        self._validate_time(occurred_at_ms, "invalid-occurred-at")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            history = self._validate_history_in_connection(con, receipt.fault_id)
            if not history.valid or history.intent is None or not history.events:
                raise ValueError("fault-history-invalid")
            intent = history.intent
            intent_row = con.execute(
                "SELECT * FROM neg_risk_fault_intents WHERE fault_id=?",
                (receipt.fault_id,),
            ).fetchone()
            assert intent_row is not None
            self._require_ownership(con, intent_row, ownership)
            tail = next(
                (event.state for event in reversed(history.events) if event.state is not None),
                None,
            )
            injected = tuple(
                event
                for event in history.events
                if event.state is FaultEventState.INJECTED
                and event.occurred_at_ms == injected_at_ms
            )
            current_runtime = self._current_runtime_in_connection(con, receipt.component)
            if (
                tail is not FaultEventState.CLEANED
                or len(injected) != 1
                or receipt.fault_id != intent.fault_id
                or receipt.kind is not intent.kind
                or receipt.call_class is not intent.call_class
                or receipt.component != intent.runtime.component
                or receipt.runtime != intent.runtime
                or current_runtime != receipt.runtime
                or receipt.writer_occurred_at_ms <= injected_at_ms
                or receipt.writer_occurred_at_ms > occurred_at_ms
            ):
                con.execute("ROLLBACK")
                return None

            recovery_id = self._validated_recovery_writer_id(
                con,
                receipt,
                intent,
            )
            if recovery_id is None:
                con.execute("ROLLBACK")
                return None
            event = self._append_event_in_transaction(
                con,
                receipt.fault_id,
                FaultEventState.RECOVERED,
                occurred_at_ms=occurred_at_ms,
                evidence={"recovery_id": recovery_id},
            )
            validated = self._validate_history_in_connection(con, receipt.fault_id)
            if not validated.valid or not validated.events or validated.events[-1] != event:
                raise ValueError("fault-recovery-invalid")
            con.execute("COMMIT")
            return event
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    @staticmethod
    def _validated_recovery_writer_id(
        con: sqlite3.Connection,
        receipt: FaultRecoveryReceipt,
        intent: FaultIntent,
    ) -> str | None:
        if receipt.writer is FaultRecoveryWriter.TELEGRAM_DELIVERY:
            if (
                receipt.component != "notification"
                or receipt.call_class is not FaultCallClass.TELEGRAM_OPPORTUNITY_CARD
                or receipt.kind is not FaultKind.TELEGRAM_FAILURE
            ):
                return None
            try:
                notification_id = int(intent.target_key)
            except (TypeError, ValueError):
                return None
            row = con.execute(
                "SELECT * FROM neg_risk_opportunity_notification_attempts "
                "WHERE id=? AND notification_id=?",
                (receipt.writer_id, notification_id),
            ).fetchone()
            latest = con.execute(
                "SELECT id FROM neg_risk_opportunity_notification_attempts "
                "WHERE notification_id=? ORDER BY id DESC LIMIT 1",
                (notification_id,),
            ).fetchone()
            notification = con.execute(
                "SELECT id FROM neg_risk_opportunity_notifications WHERE id=?",
                (notification_id,),
            ).fetchone()
            if (
                row is None
                or latest is None
                or notification is None
                or row["id"] != latest["id"]
                or row["attempted_at_ms"] != receipt.writer_occurred_at_ms
                or row["outcome"] != "delivered"
                or row["error_kind"] is not None
            ):
                return None
            return f"telegram-delivery-{row['id']}"

        if receipt.writer is FaultRecoveryWriter.CANDIDATE_SUCCESS:
            if (
                receipt.component != "candidate"
                or receipt.call_class is not FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH
                or receipt.kind
                not in {
                    FaultKind.CLOB_MISSING_LEG,
                    FaultKind.CLOB_429,
                    FaultKind.CLOB_LATENCY,
                }
                or intent.target_key == ""
            ):
                return None
            row = con.execute(
                "SELECT * FROM neg_risk_candidate_success_receipts WHERE quote_batch_id=?",
                (receipt.writer_id,),
            ).fetchone()
            if row is None:
                return None
            latest = con.execute(
                "SELECT id FROM neg_risk_candidate_success_receipts "
                "WHERE group_id=? ORDER BY id DESC LIMIT 1",
                (row["group_id"],),
            ).fetchone()
            group = con.execute(
                "SELECT * FROM neg_risk_group_revisions WHERE group_id=? "
                "ORDER BY revision DESC LIMIT 1",
                (row["group_id"],),
            ).fetchone()
            quote = con.execute(
                "SELECT rowid,* FROM neg_risk_group_quote_batches WHERE id=?",
                (row["quote_batch_id"],),
            ).fetchone()
            fact = con.execute(
                "SELECT * FROM neg_risk_candidate_watch_facts WHERE id=?",
                (row["candidate_fact_row_id"],),
            ).fetchone()
            expected_hash = candidate_success_receipt_hash(
                transaction_id=str(row["transaction_id"]),
                group_id=str(row["group_id"]),
                event_id=str(row["event_id"]),
                membership_hash=str(row["membership_hash"]),
                quote_batch_id=str(row["quote_batch_id"]),
                group_revision_row_id=int(row["group_revision_row_id"]),
                quote_batch_row_id=int(row["quote_batch_row_id"]),
                candidate_fact_row_id=int(row["candidate_fact_row_id"]),
                observed_at_ms=int(row["observed_at_ms"]),
            )
            if (
                group is None
                or latest is None
                or quote is None
                or fact is None
                or row["id"] != latest["id"]
                or row["group_id"] != intent.target_key
                or row["group_revision_row_id"] != group["id"]
                or row["membership_hash"] != group["membership_hash"]
                or row["event_id"] != group["event_id"]
                or row["quote_batch_row_id"] != quote["rowid"]
                or row["group_id"] != quote["group_id"]
                or row["membership_hash"] != quote["membership_hash"]
                or row["quote_batch_id"] != quote["id"]
                or row["candidate_fact_row_id"] != fact["id"]
                or row["group_id"] != fact["group_id"]
                or row["membership_hash"] != fact["membership_hash"]
                or row["quote_batch_id"] != fact["quote_batch_id"]
                or row["observed_at_ms"] != fact["observed_at_ms"]
                or row["observed_at_ms"] != receipt.writer_occurred_at_ms
                or row["receipt_hash"] != expected_hash
                or quote["status"] != "complete"
                or fact["last_result"] not in {"watching", "no-edge"}
            ):
                return None
            return f"candidate-success-{row['id']}"

        if receipt.writer is FaultRecoveryWriter.DISCOVERY_BATCH:
            if (
                receipt.component != "discovery"
                or receipt.call_class is not FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE
                or receipt.kind
                not in {
                    FaultKind.GAMMA_TIMEOUT,
                    FaultKind.GAMMA_MALFORMED,
                    FaultKind.GAMMA_PARTIAL,
                }
            ):
                return None
            row = con.execute(
                "SELECT * FROM neg_risk_discovery_batches WHERE id=?",
                (receipt.writer_id,),
            ).fetchone()
            latest_id = con.execute("SELECT MAX(id) FROM neg_risk_discovery_batches").fetchone()[0]
            if (
                row is None
                or row["id"] != latest_id
                or row["finished_at_ms"] != receipt.writer_occurred_at_ms
                or not (row["completed"] or row["next_cursor"] != row["requested_cursor"])
            ):
                return None
            return f"discovery-batch-{row['id']}"

        if (
            receipt.writer is not FaultRecoveryWriter.RECONCILIATION_CHECKPOINT
            or receipt.component != "reconciliation"
            or receipt.call_class is not FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE
            or receipt.kind is not FaultKind.GAMMA_CURSOR
        ):
            return None
        row = con.execute(
            "SELECT * FROM neg_risk_reconciliation_windows WHERE id=?",
            (receipt.writer_id,),
        ).fetchone()
        latest = con.execute(
            "SELECT id FROM neg_risk_reconciliation_windows "
            "ORDER BY started_at_ms DESC,rowid DESC LIMIT 1"
        ).fetchone()
        staged = con.execute(
            "SELECT * FROM neg_risk_reconciliation_staging WHERE window_id=? ORDER BY group_id",
            (receipt.writer_id,),
        ).fetchall()
        validated_checkpoint = (
            None if row is None else validate_reconciliation_authority_checkpoint(con, row, staged)
        )
        checkpoint = None if validated_checkpoint is None else validated_checkpoint[0]
        if (
            row is None
            or latest is None
            or row["id"] != latest["id"]
            or row["checkpoint_at_ms"] != receipt.writer_occurred_at_ms
            or row["pages_completed"] < 1
            or checkpoint is None
            or checkpoint["through_sequence"] < row["pages_completed"]
            or checkpoint["compacted_batch_rows"] < row["pages_completed"]
        ):
            return None
        return f"reconciliation-window-{row['id']}"

    def relinquish_claim(
        self,
        fault_id: str,
        *,
        occurred_at_ms: int,
        ownership: FaultOwnershipCapability | None,
        memory_cleared_at_ms: int | None = None,
    ) -> FaultEvent:
        """Persist the only lifecycle-valid terminal for a process-owned claim."""
        if self._read_only:
            raise RuntimeError("fault-authority-read-only")
        self._validate_time(occurred_at_ms, "invalid-occurred-at")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            history = self._validate_history_in_connection(con, fault_id)
            if not history.valid or history.intent is None or not history.events:
                raise ValueError("fault-history-invalid")
            intent_row = con.execute(
                "SELECT * FROM neg_risk_fault_intents WHERE fault_id=?",
                (fault_id,),
            ).fetchone()
            assert intent_row is not None
            self._require_ownership(con, intent_row, ownership)
            tail = next(
                (event.state for event in reversed(history.events) if event.state is not None),
                None,
            )
            if tail is FaultEventState.ARMED:
                expired = occurred_at_ms >= history.intent.accepted_at_ms + history.intent.ttl_ms
                state = FaultEventState.EXPIRED if expired else FaultEventState.ABANDONED
            elif tail in {
                FaultEventState.INJECTED,
                FaultEventState.DETECTED,
            }:
                state = FaultEventState.ABANDONED
            elif tail is FaultEventState.CONTAINED:
                state = FaultEventState.CLEANED
            else:
                raise ValueError("fault-not-relinquishable")
            evidence = (
                {"reason": "intent-expired"}
                if state is FaultEventState.EXPIRED
                else {"reason": "process-relinquished"}
                if state is FaultEventState.ABANDONED
                else {
                    "cleanup_id": secrets.token_hex(16),
                    **(
                        {
                            "memory_cleared_at_ms": str(memory_cleared_at_ms),
                            "receipt_persisted_at_ms": str(occurred_at_ms),
                        }
                        if memory_cleared_at_ms is not None
                        else {}
                    ),
                }
            )
            event = self._append_event_in_transaction(
                con,
                fault_id,
                state,
                occurred_at_ms=occurred_at_ms,
                evidence=evidence,
            )
            validated = self._validate_history_in_connection(con, fault_id)
            if not validated.valid or validated.events[-1].state is not state:
                raise ValueError("fault-terminal-invalid")
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
            state=FaultEventState(row["state"]) if row["state"] is not None else None,
            action=FaultEventAction(row["action"]) if row["action"] is not None else None,
            occurred_at_ms=row["occurred_at_ms"],
            evidence=json.loads(row["evidence_json"]),
            previous_hash=row["previous_hash"],
            event_hash=row["event_hash"],
        )

    @staticmethod
    def _nonce_row_valid(row: sqlite3.Row) -> bool:
        try:
            return row["row_hash"] == _nonce_hash(
                record_type=row["record_type"],
                nonce_digest=row["nonce_digest"],
                authorization_digest=row["authorization_digest"],
                operation=row["operation"],
                fault_id=row["fault_id"],
                request_digest=row["request_digest"],
                outcome=row["outcome"],
                reason=row["reason"],
                occurred_at_ms=row["occurred_at_ms"],
                reservation_id=row["reservation_id"],
            )
        except (KeyError, TypeError, ValueError, IndexError):
            return False

    def _auth_pair_valid(
        self,
        reservation: sqlite3.Row,
        attempt: sqlite3.Row,
        *,
        expected_operation: str,
        expected_fault_id: str,
        expected_request_digest: str,
        expected_nonce_digest: str,
        expected_authorization_digest: str,
        expected_reason: str,
    ) -> bool:
        try:
            expected = {
                "operation": expected_operation,
                "fault_id": expected_fault_id,
                "request_digest": expected_request_digest,
                "nonce_digest": expected_nonce_digest,
                "authorization_digest": expected_authorization_digest,
            }
            return bool(
                self._nonce_row_valid(reservation)
                and self._nonce_row_valid(attempt)
                and reservation["record_type"] == "reservation"
                and attempt["record_type"] == "attempt"
                and int(attempt["reservation_id"]) == int(reservation["id"])
                and attempt["outcome"] == "accepted"
                and attempt["reason"] == expected_reason
                and all(
                    reservation[key] == value and attempt[key] == value
                    for key, value in expected.items()
                )
            )
        except (KeyError, TypeError, ValueError, IndexError):
            return False

    def _auth_links_valid_for_fault(
        self,
        con: sqlite3.Connection,
        fault_id: str,
        *,
        deadline_monotonic: float | None,
    ) -> bool:
        """Reject semantic cross-link tamper with one indexed, fault-scoped query."""
        self._check_deadline(deadline_monotonic)
        mismatch = con.execute(
            "SELECT 1 FROM neg_risk_fault_auth_nonces a "
            "LEFT JOIN neg_risk_fault_auth_nonces r ON r.id=a.reservation_id "
            "WHERE a.record_type='attempt' "
            "AND (a.fault_id=? OR r.fault_id=?) AND ("
            " r.id IS NULL OR r.record_type!='reservation' "
            " OR (a.outcome='accepted' AND ("
            "   a.nonce_digest IS NOT r.nonce_digest "
            "   OR a.authorization_digest IS NOT r.authorization_digest "
            "   OR a.operation IS NOT r.operation "
            "   OR a.fault_id IS NOT r.fault_id "
            "   OR a.request_digest IS NOT r.request_digest "
            "   OR (a.operation='arm' AND a.reason!='accepted') "
            "   OR (a.operation='cleanup' AND a.reason NOT IN "
            "      ('cleanup-requested','cleanup-already-requested')) "
            "   OR (a.operation='finalize' AND a.reason NOT IN "
            "      ('verdict-finalized','verdict-already-finalized'))"
            " )) "
            " OR (a.outcome='rejected' AND a.reason!='nonce-replay' AND ("
            "   a.nonce_digest IS NOT r.nonce_digest "
            "   OR a.authorization_digest IS NOT r.authorization_digest "
            "   OR a.operation IS NOT r.operation "
            "   OR a.fault_id IS NOT r.fault_id "
            "   OR a.request_digest IS NOT r.request_digest"
            " ))"
            ") LIMIT 1",
            (fault_id, fault_id),
        ).fetchone()
        self._check_deadline(deadline_monotonic)
        return mismatch is None

    def _intent_row_valid(
        self,
        con: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        deadline_monotonic: float | None = None,
    ) -> bool:
        try:
            self._check_deadline(deadline_monotonic)
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
            reservation = con.execute(
                "SELECT * FROM neg_risk_fault_auth_nonces WHERE id=?",
                (row["auth_reservation_id"],),
            ).fetchone()
            attempt = con.execute(
                "SELECT * FROM neg_risk_fault_auth_nonces WHERE id=?",
                (row["auth_attempt_id"],),
            ).fetchone()
            self._check_deadline(deadline_monotonic)
            return bool(
                row["status"] == "accepted"
                and row["rejection_reason"] is None
                and reservation is not None
                and attempt is not None
                and attempt["occurred_at_ms"] == row["accepted_at_ms"]
                and self._auth_pair_valid(
                    reservation,
                    attempt,
                    expected_operation="arm",
                    expected_fault_id=row["fault_id"],
                    expected_request_digest=row["request_digest"],
                    expected_nonce_digest=row["nonce_digest"],
                    expected_authorization_digest=row["authorization_digest"],
                    expected_reason="accepted",
                )
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
        *,
        deadline_monotonic: float | None = None,
    ) -> FaultHistory:
        self._check_deadline(deadline_monotonic)
        row = con.execute(
            "SELECT * FROM neg_risk_fault_intents WHERE fault_id=?",
            (fault_id,),
        ).fetchone()
        if row is None:
            return FaultHistory(fault_id, False, "intent-missing", None, ())
        if not self._auth_links_valid_for_fault(
            con, fault_id, deadline_monotonic=deadline_monotonic
        ) or not self._intent_row_valid(con, row, deadline_monotonic=deadline_monotonic):
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
            self._check_deadline(deadline_monotonic)
            if event.sequence != index or event.previous_hash != previous:
                return FaultHistory(fault_id, False, "hash-predecessor-invalid", intent, events)
            try:
                if event.state is None:
                    if event.action is FaultEventAction.CLEANUP_CONFIRMED:
                        prior = events[index - 2] if index >= 2 else None
                        if (
                            prior is None
                            or prior.state is not FaultEventState.CLEANED
                            or set(event.evidence)
                            != {
                                "cleaned_event_hash",
                                "cleanup_id",
                                "memory_cleared_at_ms",
                                "receipt_commit_confirmed_at_ms",
                            }
                            or event.evidence["cleaned_event_hash"] != prior.event_hash
                            or event.evidence["cleanup_id"]
                            != prior.evidence.get("cleanup_id")
                            or int(event.evidence["memory_cleared_at_ms"])
                            != int(prior.evidence.get("memory_cleared_at_ms", -1))
                            or int(event.evidence["receipt_commit_confirmed_at_ms"])
                            != event.occurred_at_ms
                            or event.occurred_at_ms < prior.occurred_at_ms
                        ):
                            raise ValueError("invalid-cleanup-confirmation")
                        evidence_json = canonical_json(event.evidence)
                    else:
                        if event.action is not FaultEventAction.CLEANUP_REQUESTED or set(
                            event.evidence
                        ) != {
                            "authorization_digest",
                            "nonce_digest",
                            "request_digest",
                            "reservation_id",
                            "attempt_id",
                        }:
                            raise ValueError("invalid-action-evidence")
                        for key in (
                            "authorization_digest",
                            "nonce_digest",
                            "request_digest",
                        ):
                            value = event.evidence[key]
                            if (
                                not isinstance(value, str)
                                or len(value) != 64
                                or not set(value) <= set("0123456789abcdef")
                            ):
                                raise ValueError("invalid-action-evidence")
                        reservation = con.execute(
                            "SELECT * FROM neg_risk_fault_auth_nonces WHERE id=?",
                            (event.evidence["reservation_id"],),
                        ).fetchone()
                        attempt = con.execute(
                            "SELECT * FROM neg_risk_fault_auth_nonces WHERE id=?",
                            (event.evidence["attempt_id"],),
                        ).fetchone()
                        if (
                            reservation is None
                            or attempt is None
                            or attempt["occurred_at_ms"] != event.occurred_at_ms
                            or not self._auth_pair_valid(
                                reservation,
                                attempt,
                                expected_operation="cleanup",
                                expected_fault_id=fault_id,
                                expected_request_digest=event.evidence["request_digest"],
                                expected_nonce_digest=event.evidence["nonce_digest"],
                                expected_authorization_digest=event.evidence[
                                    "authorization_digest"
                                ],
                                expected_reason="cleanup-requested",
                            )
                        ):
                            raise ValueError("action-authorization-invalid")
                        evidence_json = canonical_json(event.evidence)
                else:
                    if event.action is not None:
                        raise ValueError("invalid-event-action")
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
            if event.state is None:
                if previous_state is None:
                    return FaultHistory(fault_id, False, "lifecycle-origin-invalid", intent, events)
            elif previous_state is None:
                if event.state is not expected_first:
                    return FaultHistory(fault_id, False, "lifecycle-origin-invalid", intent, events)
            elif event.state not in _NEXT_STATES.get(previous_state, frozenset()):
                return FaultHistory(fault_id, False, "lifecycle-regression", intent, events)
            previous = event.event_hash
            if event.state is not None:
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

    def _project_fault_in_connection(
        self,
        con: sqlite3.Connection,
        fault_id: str,
        *,
        now_ms: int,
        history: FaultHistory,
        deadline_monotonic: float | None,
    ) -> FaultProjection:
        if not history.valid or history.intent is None:
            return FaultProjection(fault_id, False, False, None, history.reason, history.intent)
        accepted_fault_ids = self._current_active_fault_ids(
            con, deadline_monotonic=deadline_monotonic, limit=2
        )
        active_count = 0
        for accepted_fault_id in accepted_fault_ids:
            self._check_deadline(deadline_monotonic)
            candidate = self._validate_history_in_connection(
                con,
                accepted_fault_id,
                deadline_monotonic=deadline_monotonic,
            )
            if not candidate.valid or candidate.intent is None or not candidate.events:
                return FaultProjection(
                    fault_id,
                    False,
                    False,
                    FaultEventState.EVIDENCE_INVALID,
                    "evidence-invalid",
                    history.intent,
                )
            latest_candidate = next(
                (event.state for event in reversed(candidate.events) if event.state is not None),
                None,
            )
            if latest_candidate not in _TERMINAL_STATES:
                active_count += 1
        if active_count > 1:
            return FaultProjection(
                fault_id, False, False, None, "multiple-active-chains", history.intent
            )
        current = self._current_runtime_in_connection(con, history.intent.runtime.component)
        latest = next(
            (event.state for event in reversed(history.events) if event.state is not None),
            None,
        )
        if latest is None:
            return FaultProjection(
                fault_id, False, False, None, "event-history-missing", history.intent
            )
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

    def read_snapshot(
        self,
        *,
        now_ms: int,
        fault_id: str | None = None,
        component: str | None = None,
        deadline_monotonic: float | None = None,
    ) -> FaultAuthoritySnapshot:
        """Read runtime or fault truth from one bounded SQLite snapshot."""
        self._validate_time(now_ms, "invalid-now")
        if (fault_id is None) == (component is None):
            raise ValueError("exactly-one-snapshot-selector-required")
        try:
            con = self._connect(deadline_monotonic)
            try:
                con.execute("BEGIN")
                self._check_deadline(deadline_monotonic)
                if component is not None:
                    runtime = self._current_runtime_in_connection(con, component)
                    self._check_deadline(deadline_monotonic)
                    con.execute("COMMIT")
                    if runtime is None:
                        return FaultAuthoritySnapshot(False, "runtime-evidence-unavailable")
                    return FaultAuthoritySnapshot(True, "valid", runtime=runtime)
                assert fault_id is not None
                history = self._validate_history_in_connection(
                    con, fault_id, deadline_monotonic=deadline_monotonic
                )
                projection = self._project_fault_in_connection(
                    con,
                    fault_id,
                    now_ms=now_ms,
                    history=history,
                    deadline_monotonic=deadline_monotonic,
                )
                self._check_deadline(deadline_monotonic)
                con.execute("COMMIT")
                return FaultAuthoritySnapshot(
                    projection.available and history.valid,
                    projection.reason,
                    projection=projection,
                    history=history,
                )
            except BaseException:
                self._clear_progress_handler(con)
                if con.in_transaction:
                    con.execute("ROLLBACK")
                raise
            finally:
                self._clear_progress_handler(con)
                con.close()
        except (
            sqlite3.Error,
            ValueError,
            TypeError,
            json.JSONDecodeError,
            TimeoutError,
        ):
            return FaultAuthoritySnapshot(False, "authority-unavailable")

    def project_fault(self, fault_id: str, *, now_ms: int) -> FaultProjection:
        snapshot = self.read_snapshot(now_ms=now_ms, fault_id=fault_id)
        if snapshot.projection is not None:
            return snapshot.projection
        return FaultProjection(fault_id, False, False, None, snapshot.reason, None)
