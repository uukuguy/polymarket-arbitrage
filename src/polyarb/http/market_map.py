"""Bounded, read-only HTTP models for the neg-risk opportunity watcher."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse

_READ_TIMEOUT_S = 1.0
_GROUP_LIMIT = 500
_HISTORY_LIMIT = 500


class MarketMapUnavailableError(RuntimeError):
    """No fresh, published Structure revision can support a public map."""


def _connect_read_only(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    connection.execute("PRAGMA query_only=ON")
    connection.execute("PRAGMA busy_timeout=250")
    return connection


def _read_market_map(
    db_path: Path,
    *,
    event_id: str | None,
    now_ms: int,
    max_age_s: float,
) -> dict[str, object]:
    con = _connect_read_only(db_path)
    try:
        revision = con.execute(
            "SELECT s.id,s.finished_at_ms FROM snapshots s "
            "JOIN snapshot_source_coverage c ON c.snapshot_id=s.id AND c.completed=1 "
            "WHERE s.data_product='structure' AND s.market_view_published=1 "
            "AND s.is_valid=1 ORDER BY s.id DESC LIMIT 1"
        ).fetchone()
        if revision is None:
            raise MarketMapUnavailableError()
        revision_id, finished_at_ms = int(revision[0]), int(revision[1])
        age_s = max(0.0, (now_ms - finished_at_ms) / 1000)
        if age_s > max_age_s:
            raise MarketMapUnavailableError()
        where = "WHERE snapshot_id=?"
        params: list[object] = [revision_id]
        if event_id is not None:
            where += " AND event_id=?"
            params.append(event_id)
        rows = con.execute(
            "SELECT event_id,neg_risk_market_id,quality,reason,expected_member_count,"
            "active_named_count,membership_hash FROM neg_risk_group_truth "
            f"{where} ORDER BY event_id,neg_risk_market_id LIMIT ?",
            (*params, _GROUP_LIMIT),
        ).fetchall()
        opportunities = con.execute(
            "SELECT id,event_id,group_id,status,bundle_cost,gross_edge_bps,max_bundle_size,"
            "structure_revision,quote_run_id,updated_at_ms FROM neg_risk_opportunities "
            "WHERE status='observe' "
            + ("AND event_id=? " if event_id is not None else "")
            + "ORDER BY updated_at_ms DESC,id LIMIT ?",
            (*( [event_id] if event_id is not None else []), _GROUP_LIMIT),
        ).fetchall()
    finally:
        con.close()
    scannable: list[dict[str, object]] = []
    rejected: list[dict[str, object]] = []
    for row in rows:
        group = {
            "event_id": str(row[0]),
            "group_id": str(row[1]),
            "quality": str(row[2]),
            "expected_member_count": int(row[4]),
            "active_named_count": int(row[5]),
            "membership_hash": str(row[6]),
        }
        if row[2] == "complete-supported":
            scannable.append(group)
        else:
            rejected.append(
                {
                    "event_id": group["event_id"],
                    "group_id": group["group_id"],
                    "quality": group["quality"],
                    "reason": row[3],
                }
            )
    return {
        "structure_revision": revision_id,
        "structure_age_seconds": age_s,
        "max_age_seconds": max_age_s,
        "scannable_groups": scannable,
        "rejected_groups": rejected,
        "current_opportunities": [
            {
                "id": str(row[0]),
                "event_id": str(row[1]),
                "group_id": str(row[2]),
                "status": str(row[3]),
                "bundle_cost": float(row[4]),
                "gross_edge_bps": float(row[5]),
                "max_bundle_size": float(row[6]),
                "structure_revision": int(row[7]),
                "quote_run_id": int(row[8]),
                "updated_at_ms": int(row[9]),
                "execution_status": "not-verified",
            }
            for row in opportunities
        ],
    }


def _read_history(db_path: Path, opportunity_id: str) -> dict[str, object] | None:
    con = _connect_read_only(db_path)
    try:
        master = con.execute(
            "SELECT id,event_id,group_id,status,transition_reason FROM neg_risk_opportunities "
            "WHERE id=?",
            (opportunity_id,),
        ).fetchone()
        if master is None:
            return None
        rows = con.execute(
            "SELECT observed_at_ms,source,status,reason,bundle_cost,gross_edge_bps,"
            "max_bundle_size,structure_revision,quote_run_id,legs_json "
            "FROM neg_risk_opportunity_observations WHERE opportunity_id=? "
            "ORDER BY observed_at_ms,id LIMIT ?",
            (opportunity_id, _HISTORY_LIMIT),
        ).fetchall()
    finally:
        con.close()
    return {
        "opportunity_id": str(master[0]),
        "event_id": str(master[1]),
        "group_id": str(master[2]),
        "status": str(master[3]),
        "transition_reason": master[4],
        "execution_status": "not-verified",
        "history": [
            {
                "observed_at_ms": int(row[0]),
                "source": str(row[1]),
                "status": str(row[2]),
                "reason": row[3],
                "bundle_cost": row[4],
                "gross_edge_bps": row[5],
                "max_bundle_size": row[6],
                "structure_revision": int(row[7]),
                "quote_run_id": int(row[8]),
                "legs": json.loads(row[9]),
            }
            for row in rows
        ],
    }


def durable_opportunity_ids(db_path: Path, group_ids: set[str]) -> dict[str, str]:
    """Return current observer IDs for legacy feed rows without any rescan."""
    if not group_ids:
        return {}
    placeholders = ",".join("?" for _ in group_ids)
    con = _connect_read_only(db_path)
    try:
        rows = con.execute(
            "SELECT group_id,id FROM neg_risk_opportunities "
            f"WHERE status='observe' AND group_id IN ({placeholders})",
            tuple(sorted(group_ids)),
        ).fetchall()
    finally:
        con.close()
    return {str(group_id): str(opportunity_id) for group_id, opportunity_id in rows}


async def market_map(request: Request) -> JSONResponse:
    try:
        payload = await asyncio.wait_for(
            asyncio.to_thread(
                _read_market_map,
                request.app.state.sqlite_store.db_path,
                event_id=request.query_params.get("event_id"),
                now_ms=int(time.time() * 1000),
                max_age_s=float(request.app.state.settings.market_map_max_age_s),
            ),
            timeout=_READ_TIMEOUT_S,
        )
    except (MarketMapUnavailableError, sqlite3.Error, TimeoutError, ValueError):
        return JSONResponse({"error": "market map unavailable"}, status_code=503)
    return JSONResponse(payload)


async def opportunity_watch_status(request: Request) -> JSONResponse:
    response = await market_map(request)
    if response.status_code != 200:
        return response
    payload: dict[str, Any] = json.loads(response.body)
    watcher = getattr(request.app.state, "opportunity_watcher", None)
    snapshot = watcher.snapshot() if watcher is not None else None
    payload["watcher"] = (
        {
            "reconciliation_count": snapshot.reconciliation_count,
            "last_reconciled_at_ms": snapshot.last_reconciled_at_ms,
            "notification_delivery_count": snapshot.notification_delivery_count,
            "notification_failure_count": snapshot.notification_failure_count,
            "last_notification_error_kind": snapshot.last_notification_error_kind,
        }
        if snapshot is not None
        else {"state": "unavailable"}
    )
    return JSONResponse(payload)


async def opportunity_history(request: Request) -> JSONResponse:
    try:
        payload = await asyncio.wait_for(
            asyncio.to_thread(
                _read_history,
                request.app.state.sqlite_store.db_path,
                request.path_params["opportunity_id"],
            ),
            timeout=_READ_TIMEOUT_S,
        )
    except (sqlite3.Error, TimeoutError, ValueError, json.JSONDecodeError):
        return JSONResponse({"error": "opportunity history unavailable"}, status_code=503)
    if payload is None:
        return JSONResponse({"error": "opportunity not found"}, status_code=404)
    return JSONResponse(payload)
