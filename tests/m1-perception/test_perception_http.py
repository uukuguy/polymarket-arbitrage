from __future__ import annotations

import sqlite3
import time


def test_perception_routes_exist_and_limits_are_validated(http_test_client) -> None:
    assert http_test_client.get("/perception/status").status_code == 200
    for path in (
        "/perception/groups?limit=0",
        "/perception/groups?limit=501",
        "/perception/groups?limit=1%20OR%201",
        "/perception/incidents?limit=-1",
    ):
        response = http_test_client.get(path)
        assert response.status_code == 400
        assert response.json() == {
            "status": "invalid-request",
            "reason": "limit-must-be-an-integer-from-1-to-500",
        }


def test_perception_status_distinguishes_available_zero_from_corrupt_evidence(
    http_test_client,
) -> None:
    response = http_test_client.get("/perception/status")
    assert response.status_code == 200
    assert response.json()["opportunities"] == {
        "status": "available",
        "count": 0,
        "reason": "no-certified-edge",
    }

    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_incident_events("
            "incident_id,sequence,scope,kind,state,occurred_at_ms,evidence_json"
            ") VALUES('bad',2,'candidate','worker-failure','detected',1,'{}')"
        )
    response = http_test_client.get("/perception/status")
    assert response.status_code == 503
    assert response.json()["opportunities"]["status"] == "unavailable"
    assert "traceback" not in response.text.lower()
    assert str(db_path) not in response.text


def test_group_history_is_bounded_and_corruption_fails_closed(http_test_client) -> None:
    db_path = http_test_client.app.state.sqlite_store.db_path
    now = int(time.time() * 1000)
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_group_revisions("
            "group_id,event_id,revision,membership_hash,started_at_ms,observed_at_ms,"
            "source_cursor,status,legs_json) VALUES(?,?,?,?,?,?,?,?,?)",
            ("g/quoted", "e-1", 1, "forged", now, now, "c", "certified", "[]"),
        )
    response = http_test_client.get("/perception/groups/g%2Fquoted/history?limit=10")
    assert response.status_code == 503
    assert response.json()["status"] == "unavailable"


def test_discovery_reconciliation_and_incidents_use_stable_envelopes(
    http_test_client,
) -> None:
    assert http_test_client.get("/perception/discovery").json() == {
        "status": "available",
        "discovery": None,
    }
    assert http_test_client.get("/perception/reconciliation").json() == {
        "status": "available",
        "reconciliation": None,
    }
    assert http_test_client.get("/perception/incidents?limit=5").json() == {
        "status": "available",
        "items": [],
        "limit": 5,
    }
