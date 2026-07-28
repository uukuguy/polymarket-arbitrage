"""Bounded, read-only HTTP projections for durable M1 perception facts."""

from __future__ import annotations

import asyncio
import contextvars
import json
import re
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.perception.models import GroupLeg, GroupRevision
from polyarb.perception.store import OpportunityPerceptionStore

_MAX_LIMIT = 500
_HISTORY_CAP = 500
_LEGS_JSON_MAX_BYTES = 65_536
_TIMEOUT_S = 1.0
_BUSY_TIMEOUT_MS = 250
_READ_SQL_DEADLINE_S = 0.8
_INCIDENT_EDGES = {
    "detected": {"classified"},
    "classified": {"contained", "escalated"},
    "contained": {"recovering", "escalated"},
    "recovering": {"verified", "contained", "escalated"},
    "verified": set(),
    "escalated": {"recovering"},
}
_SECRET_KEYS = {
    "secret",
    "password",
    "passwd",
    "token",
    "authorization",
    "auth",
    "api_key",
    "apikey",
    "cookie",
    "session",
    "credential",
    "dsn",
    "traceback",
    "path",
}
_INLINE_SECRET_RE = re.compile(
    r"(?i)(?:password|passwd|secret|api[_-]?key|token|authorization|cookie|session)"
    r"\s*[:=]\s*[^\s,;&]+|(?:bearer|basic)\s+[a-z0-9._~+/=-]+"
)
_READ_EXECUTION: contextvars.ContextVar[_ReadExecution | None] = contextvars.ContextVar(
    "perception_read_execution",
    default=None,
)


class _ReadExecution:
    """One absolute request deadline plus every SQLite handle it owns."""

    def __init__(self, deadline_monotonic: float) -> None:
        self.deadline_monotonic = deadline_monotonic
        self._lock = threading.Lock()
        self._connections: list[sqlite3.Connection] = []

    def register(self, con: sqlite3.Connection) -> None:
        with self._lock:
            self._connections.append(con)

    def interrupt(self) -> None:
        with self._lock:
            connections = tuple(self._connections)
        for con in connections:
            try:
                con.interrupt()
            except sqlite3.Error:
                pass

    def check(self) -> None:
        if time.monotonic() >= self.deadline_monotonic:
            raise TimeoutError("perception-read-deadline")


def _check_read_deadline() -> None:
    execution = _READ_EXECUTION.get()
    if execution is not None:
        execution.check()


def _read_store(db_path: Path) -> OpportunityPerceptionStore:
    execution = _READ_EXECUTION.get()
    return OpportunityPerceptionStore(
        db_path,
        read_only=True,
        busy_timeout_ms=_BUSY_TIMEOUT_MS,
        deadline_monotonic=(
            None if execution is None else execution.deadline_monotonic
        ),
    )


def _validate_recovery_batch(
    con: sqlite3.Connection,
    db_path: Path,
    proofs: list[
        tuple[str, int, int, dict[str, Any], dict[str, Any]]
    ],
) -> None:
    """Validate all verified incidents with fixed-count bulk reads."""
    if not proofs:
        return
    store = _read_store(db_path)
    scopes = {scope for scope, *_ in proofs}
    candidate_receipts: dict[str, sqlite3.Row] = {}
    if any(scope == "candidate" or scope.startswith("candidate:") for scope in scopes):
        store.validated_candidate_opportunity_count(_connection=con)
        candidate_receipts = {
            str(row["quote_batch_id"]): row
            for row in con.execute(
                "SELECT * FROM neg_risk_candidate_success_receipts"
            ).fetchall()
        }
    discovery_batches: dict[int, sqlite3.Row] = {}
    latest_discovery_id: int | None = None
    if "discovery" in scopes:
        discovery_rows = con.execute(
            "SELECT * FROM neg_risk_discovery_batches ORDER BY id"
        ).fetchall()
        discovery_batches = {int(row["id"]): row for row in discovery_rows}
        latest_discovery_id = None if not discovery_rows else int(discovery_rows[-1]["id"])
        if discovery_rows:
            store.discovery_status(
                max(int(row["finished_at_ms"]) for row in discovery_rows),
                _connection=con,
            )
    windows: dict[str, sqlite3.Row] = {}
    current_window = None
    if "reconciliation" in scopes:
        window_rows = con.execute(
            "SELECT * FROM neg_risk_reconciliation_windows ORDER BY rowid"
        ).fetchall()
        windows = {str(row["id"]): row for row in window_rows}
        current_window = store.current_reconciliation(_connection=con)
    probes: dict[tuple[str, str], sqlite3.Row] = {}
    if "http" in scopes:
        for row in con.execute(
            "SELECT rowid probe_row_id,* FROM neg_risk_http_probe_receipts ORDER BY id"
        ).fetchall():
            probes[(str(row["release_id"]), str(row["probe_nonce"]))] = row
    resource_rows: dict[int, sqlite3.Row] = {}
    resource_decision = None
    if "resource" in scopes:
        from polyarb.perception.resource_controller import validate_resource_history

        resource_decision = validate_resource_history(con)
        resource_rows = {
            int(row["id"]): row
            for row in con.execute(
                "SELECT * FROM neg_risk_resource_decisions"
            ).fetchall()
        }
    for scope, recovery_at, verified_at, recovery, verification in proofs:
        _check_read_deadline()
        valid = False
        if scope == "candidate" or scope.startswith("candidate:"):
            group_id = verification.get("group_id")
            receipt = candidate_receipts.get(str(verification.get("quote_batch_id")))
            anchor = recovery.get("candidate_success_receipt_row_id")
            valid = bool(
                isinstance(group_id, str)
                and group_id
                and (
                    not scope.startswith("candidate:")
                    or group_id == scope.split(":", 1)[1]
                )
                and type(anchor) is int
                and receipt is not None
                and int(receipt["id"]) > anchor
                and receipt["group_id"] == group_id
                and receipt["membership_hash"] == verification.get("membership_hash")
                and receipt["quote_batch_id"] == verification.get("quote_batch_id")
                and recovery_at <= int(receipt["observed_at_ms"]) <= verified_at
            )
        elif scope == "discovery":
            batch_id = verification.get("batch_id")
            row = (
                discovery_batches.get(batch_id)
                if type(batch_id) is int
                else None
            )
            valid = bool(
                row is not None
                and int(row["id"]) == latest_discovery_id
                and recovery_at < int(row["finished_at_ms"]) <= verified_at
                and (row["completed"] or row["next_cursor"] != row["requested_cursor"])
            )
        elif scope == "reconciliation":
            row = windows.get(str(verification.get("window_id")))
            valid = bool(
                row is not None
                and current_window is not None
                and current_window.id == row["id"]
                and recovery_at < int(row["checkpoint_at_ms"]) <= verified_at
                and int(row["pages_completed"])
                > int(recovery.get("pages_completed", -1))
            )
        elif scope == "http":
            release = recovery.get("release_id")
            nonce = recovery.get("probe_nonce")
            anchor = recovery.get("http_probe_row_id")
            row = probes.get((str(release), str(nonce)))
            valid = bool(
                isinstance(release, str)
                and isinstance(nonce, str)
                and type(anchor) is int
                and row is not None
                and row["probe_row_id"] > anchor
                and row["responsive"]
                and row["observed_release_id"] == release
                and verification.get("release_id") == release
                and verification.get("probe_nonce") == nonce
                and recovery_at <= row["started_at_ms"]
                and row["finished_at_ms"] <= verified_at
                and row["finished_at_ms"] - row["started_at_ms"] <= 2_000
            )
        elif scope == "resource":
            row_id = verification.get("decision_id")
            row = resource_rows.get(row_id) if type(row_id) is int else None
            valid = bool(
                row is not None
                and resource_decision is not None
                and resource_decision.sequence == row["sequence"]
                and resource_decision.decided_at_ms == row["decided_at_ms"]
                and recovery_at < row["decided_at_ms"] <= verified_at
            )
        if not valid:
            raise ValueError("invalid-incident-recovery-proof")


def _safe_evidence(value: Any, *, depth: int = 0) -> Any:
    if depth > 5:
        return "[redacted]"
    if isinstance(value, dict):
        result: dict[str, Any] = {}
        for key, item in list(value.items())[:100]:
            rendered_key = str(key)[:128]
            normalized = rendered_key.lower().replace("-", "_")
            result[rendered_key] = (
                "[redacted]"
                if any(marker in normalized for marker in _SECRET_KEYS)
                else _safe_evidence(item, depth=depth + 1)
            )
        return result
    if isinstance(value, list):
        return [_safe_evidence(item, depth=depth + 1) for item in value[:100]]
    if isinstance(value, str):
        if (
            "://" in value
            or _INLINE_SECRET_RE.search(value)
            or value.lower().startswith(("bearer ", "basic ", "sha256:", "/users/", "/home/"))
        ):
            return "[redacted]"
        return value[:1_024]
    if value is None or isinstance(value, (int, float, bool)):
        return value
    return "[redacted]"


def _connect(db_path: Path) -> sqlite3.Connection:
    execution = _READ_EXECUTION.get()
    con = sqlite3.connect(
        f"file:{db_path.resolve()}?mode=ro",
        uri=True,
        isolation_level=None,
        timeout=_BUSY_TIMEOUT_MS / 1_000,
    )
    con.row_factory = sqlite3.Row
    con.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    con.execute("PRAGMA query_only=ON")
    if execution is not None:
        execution.register(con)
        con.set_progress_handler(
            lambda: 1 if time.monotonic() >= execution.deadline_monotonic else 0,
            1_000,
        )
    return con


def _limit(request: Request, default: int = 100) -> int:
    raw = request.query_params.get("limit")
    if raw is None:
        return default
    try:
        value = int(raw, 10)
    except (TypeError, ValueError):
        raise ValueError("limit-must-be-an-integer-from-1-to-500") from None
    if str(value) != raw or not 1 <= value <= _MAX_LIMIT:
        raise ValueError("limit-must-be-an-integer-from-1-to-500")
    return value


def _validate_revision(row: sqlite3.Row) -> dict[str, Any]:
    try:
        raw_legs = json.loads(row["legs_json"])
        if not isinstance(raw_legs, list) or len(raw_legs) < 2:
            raise ValueError
        legs = tuple(
            GroupLeg(
                market_id=str(item[0]),
                condition_id=str(item[1]),
                yes_token_id=str(item[2]),
                title=str(item[3]),
            )
            for item in raw_legs
            if isinstance(item, list) and len(item) == 4
        )
        if len(legs) != len(raw_legs):
            raise ValueError
        if (
            not row["group_id"]
            or not row["event_id"]
            or len(str(row["group_id"])) > 256
            or len(str(row["event_id"])) > 256
            or len(str(row["source_cursor"])) > 2_048
            or len(legs) > 500
            or any(
                max(
                    len(leg.market_id),
                    len(leg.condition_id),
                    len(leg.yes_token_id),
                    len(leg.title),
                )
                > 512
                for leg in legs
            )
            or int(row["revision"]) < 1
            or int(row["started_at_ms"]) > int(row["observed_at_ms"])
            or GroupRevision.membership_digest(legs) != str(row["membership_hash"])
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise ValueError("invalid-group-history") from error
    return {
        "group_id": str(row["group_id"]),
        "event_id": str(row["event_id"]),
        "revision": int(row["revision"]),
        "membership_hash": str(row["membership_hash"]),
        "status": str(row["status"]),
        "started_at_ms": int(row["started_at_ms"]),
        "observed_at_ms": int(row["observed_at_ms"]),
        "source_cursor": str(row["source_cursor"]),
        "leg_count": len(legs),
    }


def _validate_group_history(
    rows: list[sqlite3.Row],
    *,
    allow_prefix: bool = False,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    previous_by_group: dict[str, dict[str, Any]] = {}
    allowed_transitions = {
        "discovered": {"certified", "invalidated", "closed"},
        "certified": {"certified", "stale", "invalidated", "closed"},
        "stale": {"certified", "stale", "invalidated", "closed"},
        "invalidated": {"certified", "invalidated", "closed"},
        "closed": {"closed"},
    }
    for row in rows:
        _check_read_deadline()
        item = _validate_revision(row)
        previous = previous_by_group.get(item["group_id"])
        if (
            (previous is None and item["revision"] != 1 and not allow_prefix)
            or (
                previous is not None
                and (
                    item["event_id"] != previous["event_id"]
                    or item["revision"] != previous["revision"] + 1
                    or item["started_at_ms"] < previous["started_at_ms"]
                    or item["observed_at_ms"] < previous["observed_at_ms"]
                    or item["status"]
                    not in allowed_transitions[previous["status"]]
                )
            )
        ):
            raise ValueError("invalid-group-history")
        previous_by_group[item["group_id"]] = item
        items.append(item)
    return items


def _read_incident_history(con: sqlite3.Connection, db_path: Path) -> list[dict[str, Any]]:
    count = int(con.execute("SELECT COUNT(*) FROM neg_risk_incident_events").fetchone()[0])
    evidence_size = con.execute(
        "SELECT COALESCE(MAX(length(evidence_json)),0),"
        "COALESCE(SUM(length(evidence_json)),0) "
        "FROM neg_risk_incident_events"
    ).fetchone()
    max_evidence = int(evidence_size[0])
    total_evidence = int(evidence_size[1])
    if (
        count > _HISTORY_CAP
        or max_evidence > 4_096
        or total_evidence > 1_048_576
    ):
        raise ValueError("incident-history-too-large")
    rows = con.execute(
        "SELECT incident_id,sequence,scope,kind,state,occurred_at_ms,evidence_json "
        "FROM neg_risk_incident_events ORDER BY incident_id,sequence LIMIT ?",
        (_HISTORY_CAP + 1,),
    ).fetchall()
    previous: dict[str, dict[str, Any]] = {}
    recovering: dict[str, tuple[int, dict[str, Any]]] = {}
    proofs: list[tuple[str, int, int, dict[str, Any], dict[str, Any]]] = []
    for row in rows:
        try:
            evidence = json.loads(row["evidence_json"])
            if (
                not isinstance(evidence, dict)
                or len(str(row["evidence_json"]).encode("utf-8")) > 4_096
            ):
                raise ValueError
            item = {
                "incident_id": str(row["incident_id"]),
                "sequence": int(row["sequence"]),
                "scope": str(row["scope"]),
                "kind": str(row["kind"]),
                "state": str(row["state"]),
                "occurred_at_ms": int(row["occurred_at_ms"]),
                "evidence": _safe_evidence(evidence),
            }
            prior = previous.get(item["incident_id"])
            if (prior is None and (item["sequence"] != 1 or item["state"] != "detected")) or (
                prior is not None
                and (
                    item["sequence"] != prior["sequence"] + 1
                    or item["scope"] != prior["scope"]
                    or item["kind"] != prior["kind"]
                    or item["state"] not in _INCIDENT_EDGES[prior["state"]]
                    or item["occurred_at_ms"] < prior["occurred_at_ms"]
                )
            ):
                raise ValueError
            if item["state"] == "recovering":
                recovering[item["incident_id"]] = (
                    item["occurred_at_ms"],
                    evidence,
                )
            if item["state"] == "verified":
                recovery = recovering.get(item["incident_id"])
                if recovery is None:
                    raise ValueError
                proofs.append(
                    (
                        item["scope"],
                        recovery[0],
                        item["occurred_at_ms"],
                        recovery[1],
                        evidence,
                    )
                )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("invalid-incident-history") from error
        previous[item["incident_id"]] = item
    _validate_recovery_batch(con, db_path, proofs)
    return list(previous.values())


def _status(db_path: Path) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        incidents = _read_incident_history(con, db_path)
        if any(
            item["state"] != "verified"
            and (item["scope"] == "candidate" or item["scope"].startswith("candidate:"))
            for item in incidents
        ):
            raise ValueError("candidate-worker-unavailable")
        count = _read_store(db_path).validated_candidate_opportunity_count(
            _connection=con
        )
        con.execute("COMMIT")
        return {
            "status": "available",
            "opportunities": {
                "status": "available",
                "count": count,
                "reason": "certified-edge" if count else "no-certified-edge",
            },
            "open_incident_count": sum(item["state"] != "verified" for item in incidents),
        }
    finally:
        con.close()


def _groups(db_path: Path, limit: int, after: str) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        current_meta = con.execute(
            "SELECT r.id,r.group_id,length(r.legs_json) AS legs_bytes "
            "FROM neg_risk_group_revisions r JOIN "
            "(SELECT group_id,MAX(revision) revision FROM neg_risk_group_revisions "
            "GROUP BY group_id) x ON x.group_id=r.group_id AND x.revision=r.revision "
            "WHERE r.group_id>? ORDER BY r.group_id LIMIT ?",
            (after, limit + 1),
        ).fetchall()
        if not current_meta:
            con.execute("COMMIT")
            return {
                "status": "available",
                "items": [],
                "limit": limit,
                "next_after": None,
            }
        if any(
            int(row["legs_bytes"] or 0) > _LEGS_JSON_MAX_BYTES
            for row in current_meta
        ):
            raise ValueError("group-legs-json-too-large")
        has_more = len(current_meta) > limit
        current_meta = current_meta[:limit]
        current_ids = tuple(int(row["id"]) for row in current_meta)
        current_marks = ",".join("?" for _ in current_ids)
        current = con.execute(
            f"SELECT * FROM neg_risk_group_revisions "  # noqa: S608
            f"WHERE id IN ({current_marks}) ORDER BY group_id",
            current_ids,
        ).fetchall()
        for row in current:
            revision = int(row["revision"])
            previous = (
                None
                if revision == 1
                else con.execute(
                    "SELECT * FROM neg_risk_group_revisions "
                    "WHERE group_id=? AND revision=?",
                    (row["group_id"], revision - 1),
                ).fetchone()
            )
            if revision > 1 and previous is None:
                raise ValueError("invalid-group-history")
            chain = [row] if previous is None else [previous, row]
            _validate_group_history(chain, allow_prefix=previous is not None)
        con.execute("COMMIT")
        return {
            "status": "available",
            "items": [_validate_revision(row) for row in current],
            "limit": limit,
            "next_after": (
                str(current_meta[-1]["group_id"]) if has_more else None
            ),
        }
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _history(
    db_path: Path,
    group_id: str,
    limit: int,
    before_revision: int | None,
) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        upper_revision = (
            9_223_372_036_854_775_807
            if before_revision is None
            else before_revision
        )
        rows = con.execute(
            "SELECT * FROM neg_risk_group_revisions "
            "WHERE group_id=? AND revision<? "
            "ORDER BY revision DESC LIMIT ?",
            (group_id, upper_revision, limit + 1),
        ).fetchall()
        if not rows:
            con.execute("COMMIT")
            return {
                "status": "available",
                "group_id": group_id,
                "items": [],
                "limit": limit,
                "next_before_revision": None,
            }
        has_more = len(rows) > limit
        page_rows = rows[:limit]
        oldest_revision = int(page_rows[-1]["revision"])
        anchor = (
            None
            if oldest_revision == 1
            else con.execute(
                "SELECT * FROM neg_risk_group_revisions "
                "WHERE group_id=? AND revision=?",
                (group_id, oldest_revision - 1),
            ).fetchone()
        )
        if oldest_revision > 1 and anchor is None:
            raise ValueError("invalid-group-history")
        ascending = list(reversed(page_rows))
        chain = ascending if anchor is None else [anchor, *ascending]
        validated = _validate_group_history(
            chain,
            allow_prefix=anchor is not None,
        )
        items = validated if anchor is None else validated[1:]
        items.reverse()
        con.execute("COMMIT")
        return {
            "status": "available",
            "group_id": group_id,
            "items": items,
            "limit": limit,
            "next_before_revision": (
                items[-1]["revision"] if has_more else None
            ),
        }
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()


def _discovery(db_path: Path) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        if con.execute("SELECT 1 FROM neg_risk_discovery_state WHERE id=1").fetchone() is None:
            con.execute("COMMIT")
            return {"status": "available", "discovery": None}
        status = _read_store(db_path).discovery_status(
            int(time.time() * 1_000), _connection=con
        )
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    return {
        "status": "available",
        "discovery": {
            "next_cursor": status.next_cursor,
            "completed": status.completed,
            "last_started_at_ms": status.last_started_at_ms,
            "last_finished_at_ms": status.last_finished_at_ms,
            "page_event_count": status.page_event_count,
            "groups_seen": status.groups_seen,
            "promoted_count": status.promoted_count,
            "queue_depth_by_class": status.queue_depth_by_class,
            "oldest_visit_age_ms": status.oldest_visit_age_ms,
            "promotion_queue_depth": status.promotion_queue_depth,
            "outstanding_admitted_count": status.outstanding_admitted_count,
            "candidate_start_ready": status.candidate_start_ready,
        },
    }


def _reconciliation(db_path: Path) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        con.execute("BEGIN")
        window = _read_store(db_path).current_reconciliation(_connection=con)
        con.execute("COMMIT")
    except BaseException:
        if con.in_transaction:
            con.execute("ROLLBACK")
        raise
    finally:
        con.close()
    if window is None:
        return {"status": "available", "reconciliation": None}
    return {
        "status": "available",
        "reconciliation": {
            "id": window.id,
            "status": window.status,
            "failure_reason": window.failure_reason,
            "next_cursor": window.next_cursor,
            "started_at_ms": window.started_at_ms,
            "checkpoint_at_ms": window.checkpoint_at_ms,
            "finished_at_ms": window.finished_at_ms,
            "pages_completed": window.pages_completed,
            "events_seen": window.events_seen,
            "groups_staged": window.groups_staged,
            "rejected_count": window.rejected_count,
        },
    }


def _incidents(db_path: Path, limit: int) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        latest = _read_incident_history(con, db_path)
        latest.sort(key=lambda item: (item["occurred_at_ms"], item["incident_id"]), reverse=True)
        return {"status": "available", "items": latest[:limit], "limit": limit}
    finally:
        con.close()


async def _serve(request: Request, reader: Callable[[], dict[str, Any]]) -> JSONResponse:
    execution = _ReadExecution(time.monotonic() + _READ_SQL_DEADLINE_S)
    token = _READ_EXECUTION.set(execution)
    task = asyncio.create_task(asyncio.to_thread(reader))
    _READ_EXECUTION.reset(token)
    try:
        try:
            body = await asyncio.wait_for(asyncio.shield(task), timeout=_TIMEOUT_S)
        except TimeoutError:
            execution.interrupt()
            # All production readers and nested validators share the absolute
            # SQLite/Python deadline. Awaiting convergence here prevents a
            # timed-out request from leaving a live reader thread/connection.
            try:
                await task
            except (TimeoutError, sqlite3.Error, ValueError):
                pass
            raise
        if len(
            json.dumps(
                body,
                ensure_ascii=False,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
        ) > 1_048_576:
            raise ValueError("response-output-too-large")
        return JSONResponse(body)
    except ValueError as error:
        if str(error) == "limit-must-be-an-integer-from-1-to-500":
            return JSONResponse(
                {"status": "invalid-request", "reason": str(error)}, status_code=400
            )
        return JSONResponse(
            {"status": "unavailable", "reason": "durable-evidence-invalid"},
            status_code=503,
        )
    except (TimeoutError, sqlite3.Error):
        return JSONResponse(
            {"status": "unavailable", "reason": "read-model-unavailable"},
            status_code=503,
        )


async def perception_status(request: Request) -> JSONResponse:
    db_path = Path(request.app.state.sqlite_store.db_path)
    response = await _serve(request, lambda: _status(db_path))
    if response.status_code == 503:
        return JSONResponse(
            {
                "status": "unavailable",
                "opportunities": {
                    "status": "unavailable",
                    "count": None,
                    "reason": "worker-or-evidence-unavailable",
                },
            },
            status_code=503,
        )
    return response


async def perception_groups(request: Request) -> JSONResponse:
    try:
        limit = _limit(request)
    except ValueError as error:
        return JSONResponse({"status": "invalid-request", "reason": str(error)}, status_code=400)
    after = request.query_params.get("after", "")
    if len(after) > 256 or "\x00" in after:
        return JSONResponse(
            {"status": "invalid-request", "reason": "invalid-after-cursor"},
            status_code=400,
        )
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _groups(db_path, limit, after))


async def perception_group_history(request: Request) -> JSONResponse:
    try:
        limit = _limit(request)
    except ValueError as error:
        return JSONResponse({"status": "invalid-request", "reason": str(error)}, status_code=400)
    group_id = unquote(str(request.path_params["group_id"]))
    if not group_id or len(group_id) > 256 or "\x00" in group_id:
        return JSONResponse(
            {"status": "invalid-request", "reason": "invalid-group-id"},
            status_code=400,
        )
    raw_before = request.query_params.get("before_revision")
    try:
        before_revision = None if raw_before is None else int(raw_before)
        if (
            before_revision is not None
            and (
                before_revision < 1
                or str(before_revision) != raw_before
            )
        ):
            raise ValueError
    except ValueError:
        return JSONResponse(
            {
                "status": "invalid-request",
                "reason": "before-revision-must-be-a-positive-integer",
            },
            status_code=400,
        )
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(
        request,
        lambda: _history(db_path, group_id, limit, before_revision),
    )


async def perception_discovery(request: Request) -> JSONResponse:
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _discovery(db_path))


async def perception_reconciliation(request: Request) -> JSONResponse:
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _reconciliation(db_path))


async def perception_incidents(request: Request) -> JSONResponse:
    try:
        limit = _limit(request)
    except ValueError as error:
        return JSONResponse({"status": "invalid-request", "reason": str(error)}, status_code=400)
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _incidents(db_path, limit))
