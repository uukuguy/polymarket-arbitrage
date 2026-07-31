"""Durable observer-only lifecycle facts for neg-risk opportunities."""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

from polyarb.routing.focused_quote_collector import (
    ActiveOpportunity,
    FocusedObservation,
    StructureLeg,
)
from polyarb.routing.opportunity_scanner import GroupAssessment


@dataclass(frozen=True)
class OpportunityTransition:
    opportunity_id: str
    kind: str


class FocusedObservationStaleError(RuntimeError):
    """A global reconciliation changed or closed this master before focused write."""


@dataclass(frozen=True)
class PendingNotification:
    id: int
    opportunity_id: str
    reason: str
    payload: dict[str, object]
    attempt_count: int


@dataclass(frozen=True)
class NotificationAttempt:
    id: int
    notification_id: int
    attempted_at_ms: int
    outcome: str
    error_kind: str | None


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
                            "slug": leg.slug,
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
                payload = _notification_payload(
                    assessment,
                    status="closed",
                    transition_reason="closed-gross-edge-threshold",
                )
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
                        "slug": leg.slug,
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
                payload = _notification_payload(
                    assessment,
                    status="observe",
                    transition_reason="entered-gross-edge-threshold",
                )
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
                payload = _notification_payload(
                    assessment,
                    status="observe",
                    transition_reason="edge-changed",
                )
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

    def active_masters(self) -> tuple[ActiveOpportunity, ...]:
        """Return open masters with their immutable global all-leg identity.

        Focused polling deliberately derives its request universe from the
        original global observation, then proves that the newest Structure
        still names the same legs before touching CLOB.
        """
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT o.id,o.event_id,o.group_id,o.membership_hash,"
                "o.structure_revision,opening.quote_run_id,opening.legs_json "
                "FROM neg_risk_opportunities o JOIN "
                "(SELECT opportunity_id,MIN(id) AS id "
                "FROM neg_risk_opportunity_observations WHERE source='global' "
                "GROUP BY opportunity_id) first_global "
                "ON first_global.opportunity_id=o.id "
                "JOIN neg_risk_opportunity_observations opening "
                "ON opening.id=first_global.id "
                "WHERE o.status='observe' ORDER BY o.updated_at_ms DESC,o.id"
            ).fetchall()
        finally:
            con.close()
        masters: list[ActiveOpportunity] = []
        for row in rows:
            try:
                raw_legs = json.loads(str(row[6]))
                legs = tuple(
                    StructureLeg(
                        market_id=str(leg["market_id"]),
                        condition_id=str(leg["condition_id"]),
                        slug=str(leg.get("slug", "")),
                        yes_token_id=str(leg["token_id"]),
                    )
                    for leg in raw_legs
                )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                # A corrupted historical observation cannot safely produce a
                # focused CLOB request. It remains auditable but is not active.
                continue
            if not legs or len({leg.yes_token_id for leg in legs}) != len(legs):
                continue
            masters.append(
                ActiveOpportunity(
                    id=str(row[0]),
                    event_id=str(row[1]),
                    group_id=str(row[2]),
                    membership_hash=str(row[3]),
                    structure_revision=int(row[4]),
                    quote_run_id=int(row[5]),
                    legs=legs,
                )
            )
        return tuple(masters)

    def record_focused(self, observation: FocusedObservation) -> OpportunityTransition:
        """Append a focused fact without opening masters or creating Quote runs."""
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT o.status,(SELECT quote_run_id "
                "FROM neg_risk_opportunity_observations "
                "WHERE opportunity_id=o.id AND source='global' ORDER BY id LIMIT 1) "
                "FROM neg_risk_opportunities o WHERE o.id=?",
                (observation.opportunity_id,),
            ).fetchone()
            if row is None or row[0] != "observe":
                raise FocusedObservationStaleError()
            if row[1] is None or int(row[1]) != observation.quote_run_id:
                raise FocusedObservationStaleError()

            status = observation.status
            persisted_status = "closed" if status == "no-edge" else status
            reason = observation.reason
            if status == "no-edge":
                reason = "closed-gross-edge-threshold"
                assert observation.bundle_cost is not None
                assert observation.gross_edge_bps is not None
                assert observation.max_bundle_size is not None
                con.execute(
                    "UPDATE neg_risk_opportunities SET status='closed',bundle_cost=?,"
                    "gross_edge_bps=?,max_bundle_size=?,structure_revision=?,updated_at_ms=?,"
                    "closed_at_ms=?,transition_reason=? WHERE id=?",
                    (
                        observation.bundle_cost,
                        observation.gross_edge_bps,
                        observation.max_bundle_size,
                        observation.structure_revision,
                        observation.observed_at_ms,
                        observation.observed_at_ms,
                        reason,
                        observation.opportunity_id,
                    ),
                )
                kind = "closed"
            elif status == "invalidated":
                con.execute(
                    "UPDATE neg_risk_opportunities SET status='invalidated',updated_at_ms=?,"
                    "closed_at_ms=?,transition_reason=? WHERE id=?",
                    (
                        observation.observed_at_ms,
                        observation.observed_at_ms,
                        reason,
                        observation.opportunity_id,
                    ),
                )
                kind = "invalidated"
            elif status == "observe":
                assert observation.bundle_cost is not None
                assert observation.gross_edge_bps is not None
                assert observation.max_bundle_size is not None
                con.execute(
                    "UPDATE neg_risk_opportunities SET bundle_cost=?,gross_edge_bps=?,"
                    "max_bundle_size=?,structure_revision=?,updated_at_ms=?,transition_reason=NULL "
                    "WHERE id=?",
                    (
                        observation.bundle_cost,
                        observation.gross_edge_bps,
                        observation.max_bundle_size,
                        observation.structure_revision,
                        observation.observed_at_ms,
                        observation.opportunity_id,
                    ),
                )
                kind = "observed"
            else:  # unavailable remains retriable; the append-only fact carries its state.
                con.execute(
                    "UPDATE neg_risk_opportunities "
                    "SET updated_at_ms=?,transition_reason=? WHERE id=?",
                    (observation.observed_at_ms, reason, observation.opportunity_id),
                )
                kind = "unavailable"

            legs_json = json.dumps(
                [
                    {
                        "market_id": leg.market_id,
                        "condition_id": leg.condition_id,
                        "token_id": leg.yes_token_id,
                        "ask": leg.ask_price,
                        "ask_size": leg.ask_size,
                    }
                    for leg in observation.legs
                ],
                separators=(",", ":"),
                sort_keys=True,
            )
            con.execute(
                "INSERT INTO neg_risk_opportunity_observations("
                "opportunity_id,observed_at_ms,source,status,reason,bundle_cost,"
                "gross_edge_bps,max_bundle_size,structure_revision,quote_run_id,legs_json"
                ") VALUES (?,?,'focused',?,?,?,?,?,?,?,?)",
                (
                    observation.opportunity_id,
                    observation.observed_at_ms,
                    persisted_status,
                    reason,
                    observation.bundle_cost,
                    observation.gross_edge_bps,
                    observation.max_bundle_size,
                    observation.structure_revision,
                    observation.quote_run_id,
                    legs_json,
                ),
            )
            con.execute("COMMIT")
            return OpportunityTransition(opportunity_id=observation.opportunity_id, kind=kind)
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def pending_notifications(self, *, now_ms: int) -> tuple[PendingNotification, ...]:
        del now_ms  # The first version has no backoff window; delivery owns retry policy.
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT n.id,n.opportunity_id,n.reason,n.payload_json,"
                "n.attempt_count+(SELECT COUNT(*) "
                "FROM neg_risk_opportunity_notification_attempts a "
                "WHERE a.notification_id=n.id) "
                "FROM neg_risk_opportunity_notifications n "
                "WHERE n.status != 'delivered' AND NOT EXISTS("
                "SELECT 1 FROM neg_risk_opportunity_notification_attempts a "
                "WHERE a.notification_id=n.id AND a.outcome='delivered'"
                ") ORDER BY n.created_at_ms,n.id"
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

    def mark_notification_delivered(
        self,
        notification_id: int,
        *,
        delivered_at_ms: int,
    ) -> NotificationAttempt | None:
        """Append a delivery attempt without changing its immutable intent."""
        con = self._connect()
        try:
            row = con.execute(
                "INSERT INTO neg_risk_opportunity_notification_attempts("
                "notification_id,attempted_at_ms,outcome,error_kind"
                ") SELECT ?,?,'delivered',NULL WHERE EXISTS("
                "SELECT 1 FROM neg_risk_opportunity_notifications "
                "WHERE id=? AND status != 'delivered'"
                ") AND NOT EXISTS("
                "SELECT 1 FROM neg_risk_opportunity_notification_attempts "
                "WHERE notification_id=? AND outcome='delivered'"
                ") RETURNING id,notification_id,attempted_at_ms,outcome,error_kind",
                (notification_id, delivered_at_ms, notification_id, notification_id),
            ).fetchone()
        finally:
            con.close()
        return self._attempt_from_row(row)

    def mark_notification_failed(
        self,
        notification_id: int,
        *,
        attempted_at_ms: int,
        error_kind: str,
    ) -> NotificationAttempt | None:
        """Append a retryable failed attempt without touching market state."""
        con = self._connect()
        try:
            row = con.execute(
                "INSERT INTO neg_risk_opportunity_notification_attempts("
                "notification_id,attempted_at_ms,outcome,error_kind"
                ") SELECT ?,?,'failed',? WHERE EXISTS("
                "SELECT 1 FROM neg_risk_opportunity_notifications "
                "WHERE id=? AND status != 'delivered'"
                ") AND NOT EXISTS("
                "SELECT 1 FROM neg_risk_opportunity_notification_attempts "
                "WHERE notification_id=? AND outcome='delivered'"
                ") RETURNING id,notification_id,attempted_at_ms,outcome,error_kind",
                (
                    notification_id,
                    attempted_at_ms,
                    error_kind,
                    notification_id,
                    notification_id,
                ),
            ).fetchone()
        finally:
            con.close()
        return self._attempt_from_row(row)

    def notification_attempts(self, notification_id: int) -> tuple[NotificationAttempt, ...]:
        """Return append-only delivery evidence in its original order."""
        con = self._connect()
        try:
            rows = con.execute(
                "SELECT id,notification_id,attempted_at_ms,outcome,error_kind "
                "FROM neg_risk_opportunity_notification_attempts "
                "WHERE notification_id=? ORDER BY attempted_at_ms,id",
                (notification_id,),
            ).fetchall()
        finally:
            con.close()
        return tuple(
            self._attempt_from_row(row)
            for row in rows
        )

    @staticmethod
    def _attempt_from_row(row: sqlite3.Row | tuple | None) -> NotificationAttempt | None:
        if row is None:
            return None
        return NotificationAttempt(
            id=int(row[0]),
            notification_id=int(row[1]),
            attempted_at_ms=int(row[2]),
            outcome=str(row[3]),
            error_kind=str(row[4]) if row[4] is not None else None,
        )


def _notification_payload(
    assessment: GroupAssessment,
    *,
    status: str,
    transition_reason: str,
) -> dict[str, object]:
    """Freeze every available global provenance fact into the outbox intent."""
    return {
        "status": status,
        "strategy": "neg-risk-buy-all",
        "event_id": assessment.event_id,
        "group_id": assessment.group_id,
        "membership_hash": assessment.membership_hash,
        "bundle_cost": assessment.bundle_cost,
        "gross_edge_bps": assessment.gross_edge_bps,
        "max_bundle_size": assessment.max_bundle_size,
        "structure_revision": assessment.structure_revision,
        "quote_run_id": assessment.quote_run_id,
        "quoted_at_ms": assessment.quoted_at_ms,
        "legs": [
            {
                "market_id": leg.market_id,
                "condition_id": leg.condition_id,
                "slug": leg.slug,
                "token_id": leg.yes_token_id,
                "ask": leg.ask_price,
                "ask_size": leg.ask_size,
            }
            for leg in assessment.legs
        ],
        "transition_reason": transition_reason,
        "execution_status": "not-verified",
    }
