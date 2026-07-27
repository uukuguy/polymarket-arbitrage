"""Durable observer-only lifecycle facts for neg-risk opportunities."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

from polyarb.routing.opportunity_scanner import GroupAssessment


@dataclass(frozen=True)
class OpportunityTransition:
    opportunity_id: str
    kind: str


@dataclass(frozen=True)
class PendingNotification:
    id: int
    opportunity_id: str
    reason: str
    payload: dict[str, object]
    attempt_count: int


class OpportunityLedger:
    """SQLite ledger where a market transition and its alert intent commit together."""

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path, isolation_level=None)
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def reconcile_global(
        self,
        assessment: GroupAssessment,
        *,
        observed_at_ms: int,
    ) -> OpportunityTransition:
        """Append one global observation and transition the matching master."""
        if assessment.status not in ("observe", "no-edge"):
            raise ValueError("global ledger requires observe or no-edge assessment")
        if assessment.event_id is None or assessment.membership_hash is None:
            raise ValueError("observe assessment requires verified identity")
        if (
            assessment.bundle_cost is None
            or assessment.gross_edge_bps is None
            or assessment.max_bundle_size is None
        ):
            raise ValueError("observe assessment requires complete economics")

        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT id,gross_edge_bps,max_bundle_size FROM neg_risk_opportunities "
                "WHERE event_id=? AND group_id=? AND membership_hash=? AND status='observe'",
                (assessment.event_id, assessment.group_id, assessment.membership_hash),
            ).fetchone()
            if assessment.status == "no-edge":
                if row is None:
                    con.execute("COMMIT")
                    return OpportunityTransition(opportunity_id="", kind="no-active-opportunity")
                opportunity_id = str(row[0])
                legs_json = json.dumps(
                    [
                        {
                            "market_id": leg.market_id,
                            "condition_id": leg.condition_id,
                            "token_id": leg.yes_token_id,
                            "ask": leg.ask_price,
                            "ask_size": leg.ask_size,
                        }
                        for leg in assessment.legs
                    ],
                    separators=(",", ":"),
                    sort_keys=True,
                )
                con.execute(
                    "UPDATE neg_risk_opportunities SET status='closed',bundle_cost=?,"
                    "gross_edge_bps=?,max_bundle_size=?,structure_revision=?,quote_run_id=?,"
                    "updated_at_ms=?,closed_at_ms=?,transition_reason=? WHERE id=?",
                    (
                        assessment.bundle_cost,
                        assessment.gross_edge_bps,
                        assessment.max_bundle_size,
                        assessment.structure_revision,
                        assessment.quote_run_id,
                        observed_at_ms,
                        observed_at_ms,
                        "closed-gross-edge-threshold",
                        opportunity_id,
                    ),
                )
                con.execute(
                    "INSERT INTO neg_risk_opportunity_observations("
                    "opportunity_id,observed_at_ms,source,status,reason,bundle_cost,"
                    "gross_edge_bps,max_bundle_size,structure_revision,quote_run_id,legs_json"
                    ") VALUES (?,?, 'global','closed',?,?,?,?,?,?,?)",
                    (
                        opportunity_id,
                        observed_at_ms,
                        "closed-gross-edge-threshold",
                        assessment.bundle_cost,
                        assessment.gross_edge_bps,
                        assessment.max_bundle_size,
                        assessment.structure_revision,
                        assessment.quote_run_id,
                        legs_json,
                    ),
                )
                payload = {
                    "status": "closed",
                    "event_id": assessment.event_id,
                    "group_id": assessment.group_id,
                    "gross_edge_bps": assessment.gross_edge_bps,
                    "execution_status": "not-verified",
                }
                con.execute(
                    "INSERT INTO neg_risk_opportunity_notifications("
                    "opportunity_id,reason,payload_json,status,created_at_ms"
                    ") VALUES (?,? ,?,'pending',?)",
                    (
                        opportunity_id,
                        "closed-gross-edge-threshold",
                        json.dumps(payload, separators=(",", ":"), sort_keys=True),
                        observed_at_ms,
                    ),
                )
                con.execute("COMMIT")
                return OpportunityTransition(opportunity_id=opportunity_id, kind="closed")
            if row is None:
                opportunity_id = uuid.uuid4().hex
                con.execute(
                    "INSERT INTO neg_risk_opportunities("
                    "id,event_id,group_id,membership_hash,status,bundle_cost,gross_edge_bps,"
                    "max_bundle_size,structure_revision,quote_run_id,opened_at_ms,updated_at_ms"
                    ") VALUES (?,?,?,?, 'observe',?,?,?,?,?,?,?)",
                    (
                        opportunity_id,
                        assessment.event_id,
                        assessment.group_id,
                        assessment.membership_hash,
                        assessment.bundle_cost,
                        assessment.gross_edge_bps,
                        assessment.max_bundle_size,
                        assessment.structure_revision,
                        assessment.quote_run_id,
                        observed_at_ms,
                        observed_at_ms,
                    ),
                )
                kind = "entered"
            else:
                opportunity_id = str(row[0])
                previous_edge_bps = float(row[1])
                con.execute(
                    "UPDATE neg_risk_opportunities SET bundle_cost=?,gross_edge_bps=?,"
                    "max_bundle_size=?,structure_revision=?,quote_run_id=?,updated_at_ms=? "
                    "WHERE id=?",
                    (
                        assessment.bundle_cost,
                        assessment.gross_edge_bps,
                        assessment.max_bundle_size,
                        assessment.structure_revision,
                        assessment.quote_run_id,
                        observed_at_ms,
                        opportunity_id,
                    ),
                )
                kind = (
                    "edge-changed"
                    if abs(assessment.gross_edge_bps - previous_edge_bps) >= 25
                    else "unchanged"
                )

            legs_json = json.dumps(
                [
                    {
                        "market_id": leg.market_id,
                        "condition_id": leg.condition_id,
                        "token_id": leg.yes_token_id,
                        "ask": leg.ask_price,
                        "ask_size": leg.ask_size,
                    }
                    for leg in assessment.legs
                ],
                separators=(",", ":"),
                sort_keys=True,
            )
            con.execute(
                "INSERT INTO neg_risk_opportunity_observations("
                "opportunity_id,observed_at_ms,source,status,reason,bundle_cost,"
                "gross_edge_bps,max_bundle_size,structure_revision,quote_run_id,legs_json"
                ") VALUES (?,?, 'global','observe',NULL,?,?,?,?,?,?)",
                (
                    opportunity_id,
                    observed_at_ms,
                    assessment.bundle_cost,
                    assessment.gross_edge_bps,
                    assessment.max_bundle_size,
                    assessment.structure_revision,
                    assessment.quote_run_id,
                    legs_json,
                ),
            )
            if kind == "entered":
                payload = {
                    "status": "observe",
                    "strategy": "neg-risk-buy-all",
                    "event_id": assessment.event_id,
                    "group_id": assessment.group_id,
                    "bundle_cost": assessment.bundle_cost,
                    "gross_edge_bps": assessment.gross_edge_bps,
                    "max_bundle_size": assessment.max_bundle_size,
                    "structure_revision": assessment.structure_revision,
                    "quote_run_id": assessment.quote_run_id,
                    "execution_status": "not-verified",
                }
                con.execute(
                    "INSERT INTO neg_risk_opportunity_notifications("
                    "opportunity_id,reason,payload_json,status,created_at_ms"
                    ") VALUES (?,? ,?,'pending',?)",
                    (
                        opportunity_id,
                        "entered-gross-edge-threshold",
                        json.dumps(payload, separators=(",", ":"), sort_keys=True),
                        observed_at_ms,
                    ),
                )
            elif kind == "edge-changed":
                payload = {
                    "status": "observe",
                    "strategy": "neg-risk-buy-all",
                    "event_id": assessment.event_id,
                    "group_id": assessment.group_id,
                    "bundle_cost": assessment.bundle_cost,
                    "gross_edge_bps": assessment.gross_edge_bps,
                    "max_bundle_size": assessment.max_bundle_size,
                    "structure_revision": assessment.structure_revision,
                    "quote_run_id": assessment.quote_run_id,
                    "execution_status": "not-verified",
                }
                con.execute(
                    "INSERT INTO neg_risk_opportunity_notifications("
                    "opportunity_id,reason,payload_json,status,created_at_ms"
                    ") VALUES (?,? ,?,'pending',?)",
                    (
                        opportunity_id,
                        "edge-changed",
                        json.dumps(payload, separators=(",", ":"), sort_keys=True),
                        observed_at_ms,
                    ),
                )
            con.execute("COMMIT")
            return OpportunityTransition(opportunity_id=opportunity_id, kind=kind)
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def current_opportunities(self) -> list[dict[str, object]]:
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT id,status,event_id,group_id,membership_hash,bundle_cost,"
                "gross_edge_bps,max_bundle_size,structure_revision,quote_run_id "
                "FROM neg_risk_opportunities WHERE status='observe' "
                "ORDER BY updated_at_ms DESC,id"
            ).fetchall()
        finally:
            con.close()
        keys = (
            "id",
            "status",
            "event_id",
            "group_id",
            "membership_hash",
            "bundle_cost",
            "gross_edge_bps",
            "max_bundle_size",
            "structure_revision",
            "quote_run_id",
        )
        return [dict(zip(keys, row, strict=True)) for row in rows]

    def pending_notifications(self, *, now_ms: int) -> tuple[PendingNotification, ...]:
        del now_ms  # The first version has no backoff window; delivery owns retry policy.
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT id,opportunity_id,reason,payload_json,attempt_count "
                "FROM neg_risk_opportunity_notifications WHERE status IN ('pending','failed') "
                "ORDER BY created_at_ms,id"
            ).fetchall()
        finally:
            con.close()
        return tuple(
            PendingNotification(
                id=int(row[0]),
                opportunity_id=str(row[1]),
                reason=str(row[2]),
                payload=json.loads(row[3]),
                attempt_count=int(row[4]),
            )
            for row in rows
        )
