from __future__ import annotations

import sqlite3
from pathlib import Path

from polyarb.perception.notification_incidents import (
    NotificationIncidents,
    QualifiedNotificationIncidentReceipt,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.storage.sqlite_store import SQLiteStore


def _store(tmp_path: Path) -> OpportunityPerceptionStore:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_opportunities("
            "id,event_id,group_id,membership_hash,status,bundle_cost,"
            "gross_edge_bps,max_bundle_size,structure_revision,quote_run_id,"
            "opened_at_ms,updated_at_ms,closed_at_ms,transition_reason"
            ") VALUES ('opp','event','group','membership','observe',"
            "0.9,100,1,1,1,900,900,NULL,NULL)"
        )
        con.executemany(
            "INSERT INTO neg_risk_opportunity_notifications("
            "id,opportunity_id,reason,payload_json,status,attempt_count,created_at_ms"
            ") VALUES (?,'opp','opened','{}','pending',0,900)",
            [(1,), (2,)],
        )
        con.executemany(
            "INSERT INTO neg_risk_opportunity_notification_attempts("
            "id,notification_id,attempted_at_ms,outcome,error_kind"
            ") VALUES (?,?,?,?,?)",
            [
                (10, 1, 1_001, "failed", "QualifiedTelegramTransportError"),
                (11, 2, 1_002, "failed", "OSError"),
                (12, 1, 1_003, "delivered", None),
            ],
        )
    return OpportunityPerceptionStore(db_path)


def test_qualified_failure_receipt_binds_first_incident_event_to_exact_call(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    incidents = NotificationIncidents(store, clock_ms=lambda: 1_001)

    receipt = incidents.record_qualified_failure(
        notification_id=1,
        failed_attempt_id=10,
        error_kind="QualifiedTelegramTransportError",
        fault_call_id="call-telegram",
    )

    assert isinstance(receipt, QualifiedNotificationIncidentReceipt)
    assert receipt.scope == "notification:1"
    assert receipt.kind == "telegram-delivery-failed"
    assert receipt.fault_call_id == "call-telegram"
    assert incidents.validate_qualified_receipt(receipt)


def test_preexisting_or_other_notification_incident_cannot_claim_call_id(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    incidents = NotificationIncidents(store, clock_ms=lambda: 1_001)
    incidents.record_failure(
        notification_id=1,
        failed_attempt_id=10,
        error_kind="OSError",
    )

    assert (
        incidents.record_qualified_failure(
            notification_id=1,
            failed_attempt_id=10,
            error_kind="QualifiedTelegramTransportError",
            fault_call_id="call-telegram",
        )
        is None
    )
    receipt = incidents.record_qualified_failure(
        notification_id=2,
        failed_attempt_id=11,
        error_kind="QualifiedTelegramTransportError",
        fault_call_id="call-other",
    )
    assert receipt is not None
    assert receipt.scope == "notification:2"


def test_delivery_verification_returns_exact_authoritative_attempt(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    now_ms = [1_001]
    incidents = NotificationIncidents(store, clock_ms=lambda: now_ms[0])
    incidents.record_failure(
        notification_id=1,
        failed_attempt_id=10,
        error_kind="OSError",
    )
    now_ms[0] = 1_004

    attempt = incidents.verify_delivery(
        notification_id=1,
        delivered_attempt_id=12,
    )

    assert attempt.id == 12
    assert attempt.notification_id == 1
    assert attempt.outcome == "delivered"
    assert store.open_incidents() == ()
