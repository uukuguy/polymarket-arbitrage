"""Bounded, read-only HTTP projections for durable M1 perception facts."""

from __future__ import annotations

import asyncio
import hmac
import json
import math
import sqlite3
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import unquote

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.perception.incidents import Incident, IncidentManager
from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.store import (
    OpportunityPerceptionStore,
    candidate_success_receipt_hash,
)

_MAX_LIMIT = 500
_HISTORY_CAP = 500
_TIMEOUT_S = 1.0
_BUSY_TIMEOUT_MS = 250
_INCIDENT_EDGES = {
    "detected": {"classified"},
    "classified": {"contained", "escalated"},
    "contained": {"recovering", "escalated"},
    "recovering": {"verified", "contained", "escalated"},
    "verified": set(),
    "escalated": {"recovering"},
}
_SECRET_KEYS = {"secret", "password", "token", "authorization", "dsn", "traceback", "path"}


def _safe_evidence(value: Any, *, depth: int = 0) -> Any:
    if depth > 5:
        return "[redacted]"
    if isinstance(value, dict):
        return {
            str(key): (
                "[redacted]"
                if any(marker in str(key).lower() for marker in _SECRET_KEYS)
                else _safe_evidence(item, depth=depth + 1)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_safe_evidence(item, depth=depth + 1) for item in value[:100]]
    if isinstance(value, str) and (
        "://" in value or value.lower().startswith(("bearer ", "sha256:", "/users/", "/home/"))
    ):
        return "[redacted]"
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return "[redacted]"


def _connect(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(
        f"file:{db_path.resolve()}?mode=ro",
        uri=True,
        isolation_level=None,
        timeout=_BUSY_TIMEOUT_MS / 1_000,
    )
    con.row_factory = sqlite3.Row
    con.execute(f"PRAGMA busy_timeout={_BUSY_TIMEOUT_MS}")
    con.execute("PRAGMA query_only=ON")
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


def _read_incident_history(con: sqlite3.Connection, db_path: Path) -> list[dict[str, Any]]:
    count = int(con.execute("SELECT COUNT(*) FROM neg_risk_incident_events").fetchone()[0])
    if count > _HISTORY_CAP:
        raise ValueError("incident-history-too-large")
    rows = con.execute(
        "SELECT incident_id,sequence,scope,kind,state,occurred_at_ms,evidence_json "
        "FROM neg_risk_incident_events ORDER BY incident_id,sequence LIMIT ?",
        (_HISTORY_CAP + 1,),
    ).fetchall()
    previous: dict[str, dict[str, Any]] = {}
    recovering: dict[str, tuple[int, dict[str, Any]]] = {}
    manager = IncidentManager(OpportunityPerceptionStore(db_path, read_only=True))
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
                incident = Incident(
                    id=item["incident_id"],
                    sequence=item["sequence"],
                    scope=item["scope"],
                    kind=item["kind"],
                    state="verified",
                    occurred_at_ms=item["occurred_at_ms"],
                    evidence=evidence,
                )
                if recovery is None or not manager._has_recovery_proof(
                    con,
                    incident,
                    recovery_started_at_ms=recovery[0],
                    verification_at_ms=item["occurred_at_ms"],
                    recovery_evidence=recovery[1],
                    verification_evidence=evidence,
                ):
                    raise ValueError
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise ValueError("invalid-incident-history") from error
        previous[item["incident_id"]] = item
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
        fact_count = int(
            con.execute(
                "SELECT COUNT(*) FROM (SELECT group_id FROM "
                "neg_risk_candidate_watch_facts GROUP BY group_id)"
            ).fetchone()[0]
        )
        if fact_count > _MAX_LIMIT:
            raise ValueError("opportunity-output-too-large")
        rows = con.execute(
            "WITH latest_fact AS (SELECT f.* FROM neg_risk_candidate_watch_facts f "
            "JOIN (SELECT group_id,MAX(id) id FROM neg_risk_candidate_watch_facts "
            "GROUP BY group_id) x ON x.id=f.id), current_group AS ("
            "SELECT r.* FROM neg_risk_group_revisions r JOIN "
            "(SELECT group_id,MAX(revision) revision FROM neg_risk_group_revisions "
            "GROUP BY group_id) x ON x.group_id=r.group_id AND x.revision=r.revision) "
            "SELECT f.*,g.id group_revision_row_id,g.event_id,g.status group_status,"
            "g.legs_json group_legs_json,r.transaction_id,r.quote_batch_row_id,"
            "r.receipt_hash,q.rowid joined_quote_row_id,q.status quote_status,"
            "q.started_at_ms quote_started_at_ms,q.quoted_at_ms,q.legs_json quote_legs_json "
            "FROM latest_fact f LEFT JOIN current_group g ON g.group_id=f.group_id "
            "AND g.membership_hash=f.membership_hash "
            "LEFT JOIN neg_risk_candidate_success_receipts r "
            "ON r.candidate_fact_row_id=f.id "
            "LEFT JOIN neg_risk_group_quote_batches q ON q.id=f.quote_batch_id "
            "ORDER BY f.group_id LIMIT 501"
        ).fetchall()
        if len(rows) > _MAX_LIMIT:
            raise ValueError("opportunity-output-too-large")
        opportunity_count = 0
        for row in rows:
            result = str(row["last_result"])
            if (
                result not in {"watching", "no-edge", "unavailable"}
                or int(row["observed_at_ms"]) < 0
                or int(row["next_due_at_ms"]) < int(row["observed_at_ms"])
                or not math.isfinite(float(row["effective_interval_s"]))
                or float(row["effective_interval_s"]) <= 0
            ):
                raise ValueError("invalid-opportunity-fact")
            if result in {"watching", "no-edge"}:
                numeric = ("bundle_cost", "gross_edge_bps", "max_bundle_size")
                if (
                    row["group_revision_row_id"] is None
                    or row["group_status"] != "certified"
                    or row["transaction_id"] is None
                    or row["quote_status"] != "complete"
                    or int(row["quote_batch_row_id"]) != int(row["joined_quote_row_id"])
                    or not all(
                        row[key] is not None and math.isfinite(float(row[key])) for key in numeric
                    )
                ):
                    raise ValueError("invalid-opportunity-fact")
                quote_legs_raw = json.loads(row["quote_legs_json"])
                quote_legs = tuple(
                    GroupQuoteLeg(
                        yes_token_id=str(item[0]),
                        membership_hash=str(row["membership_hash"]),
                        best_ask_price=float(item[2]),
                        best_ask_size=float(item[3]),
                        terminal_state=str(item[4]),
                    )
                    for item in quote_legs_raw
                    if isinstance(item, list) and len(item) == 5
                )
                if len(quote_legs) != len(quote_legs_raw):
                    raise ValueError("invalid-opportunity-fact")
                GroupQuoteBatch.complete(
                    group_id=str(row["group_id"]),
                    membership_hash=str(row["membership_hash"]),
                    quote_batch_id=str(row["quote_batch_id"]),
                    started_at_ms=int(row["quote_started_at_ms"]),
                    quoted_at_ms=int(row["quoted_at_ms"]),
                    legs=quote_legs,
                )
                expected_hash = candidate_success_receipt_hash(
                    transaction_id=str(row["transaction_id"]),
                    group_id=str(row["group_id"]),
                    event_id=str(row["event_id"]),
                    membership_hash=str(row["membership_hash"]),
                    quote_batch_id=str(row["quote_batch_id"]),
                    group_revision_row_id=int(row["group_revision_row_id"]),
                    quote_batch_row_id=int(row["quote_batch_row_id"]),
                    candidate_fact_row_id=int(row["id"]),
                    observed_at_ms=int(row["observed_at_ms"]),
                )
                if not hmac.compare_digest(str(row["receipt_hash"]), expected_hash):
                    raise ValueError("invalid-opportunity-fact")
                if result == "watching" and float(row["gross_edge_bps"]) > 0:
                    opportunity_count += 1
        con.execute("COMMIT")
        count = opportunity_count
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


def _groups(db_path: Path, limit: int) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        current = con.execute(
            "SELECT r.* FROM neg_risk_group_revisions r JOIN "
            "(SELECT group_id,MAX(revision) revision FROM neg_risk_group_revisions "
            "GROUP BY group_id) x ON x.group_id=r.group_id AND x.revision=r.revision "
            "ORDER BY r.group_id LIMIT ?",
            (limit,),
        ).fetchall()
        if not current:
            return {"status": "available", "items": [], "limit": limit}
        placeholders = ",".join("?" for _ in current)
        group_ids = tuple(str(row["group_id"]) for row in current)
        count = int(
            con.execute(
                f"SELECT COUNT(*) FROM neg_risk_group_revisions "  # noqa: S608
                f"WHERE group_id IN ({placeholders})",
                group_ids,
            ).fetchone()[0]
        )
        if count > _HISTORY_CAP:
            raise ValueError("group-history-too-large")
        histories = con.execute(
            f"SELECT * FROM neg_risk_group_revisions "  # noqa: S608
            f"WHERE group_id IN ({placeholders}) ORDER BY group_id,revision LIMIT ?",
            (*group_ids, _HISTORY_CAP + 1),
        ).fetchall()
        prior: dict[str, int] = {}
        for row in histories:
            item = _validate_revision(row)
            if item["group_id"] in prior and item["revision"] <= prior[item["group_id"]]:
                raise ValueError("invalid-group-history")
            prior[item["group_id"]] = item["revision"]
        return {
            "status": "available",
            "items": [_validate_revision(row) for row in current],
            "limit": limit,
        }
    finally:
        con.close()


def _history(db_path: Path, group_id: str, limit: int) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        count = int(
            con.execute(
                "SELECT COUNT(*) FROM neg_risk_group_revisions WHERE group_id=?",
                (group_id,),
            ).fetchone()[0]
        )
        if count > _HISTORY_CAP:
            raise ValueError("group-history-too-large")
        rows = con.execute(
            "SELECT * FROM neg_risk_group_revisions WHERE group_id=? ORDER BY revision LIMIT ?",
            (group_id, _HISTORY_CAP + 1),
        ).fetchall()
        items = [_validate_revision(row) for row in rows]
        for prior, item in zip(items, items[1:], strict=False):
            if item["revision"] <= prior["revision"]:
                raise ValueError("invalid-group-history")
        return {
            "status": "available",
            "group_id": group_id,
            "items": list(reversed(items))[:limit],
            "limit": limit,
        }
    finally:
        con.close()


def _discovery(db_path: Path) -> dict[str, Any]:
    con = _connect(db_path)
    try:
        if con.execute("SELECT 1 FROM neg_risk_discovery_state WHERE id=1").fetchone() is None:
            return {"status": "available", "discovery": None}
    finally:
        con.close()
    status = OpportunityPerceptionStore(
        db_path,
        read_only=True,
        busy_timeout_ms=_BUSY_TIMEOUT_MS,
    ).discovery_status(int(time.time() * 1_000))
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
    window = OpportunityPerceptionStore(
        db_path,
        read_only=True,
        busy_timeout_ms=_BUSY_TIMEOUT_MS,
    ).current_reconciliation()
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
    try:
        body = await asyncio.wait_for(asyncio.to_thread(reader), timeout=_TIMEOUT_S)
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
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _groups(db_path, limit))


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
    db_path = Path(request.app.state.sqlite_store.db_path)
    return await _serve(request, lambda: _history(db_path, group_id, limit))


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
