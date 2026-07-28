from __future__ import annotations

import hashlib
import hmac
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

_SECRET = "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"


def _signed(
    client,
    path: str,
    *,
    secret: str = _SECRET,
    nonce: str | None = None,
    body: bytes = b"{}",
    timestamp: str | None = None,
    signed_body: bytes | None = None,
):
    timestamp = timestamp or str(int(time.time()))
    nonce = nonce or uuid.uuid4().hex
    canonical = b"\n".join(
        (
            timestamp.encode(),
            nonce.encode(),
            b"POST",
            path.encode(),
            body if signed_body is None else signed_body,
        )
    )
    signature = hmac.new(secret.encode(), canonical, hashlib.sha256).hexdigest()
    return client.post(
        path,
        content=body,
        headers={
            "Content-Type": "application/json",
            "X-Perception-Timestamp": timestamp,
            "X-Perception-Nonce": nonce,
            "X-Signature": f"sha256={signature}",
        },
    )


def test_perception_controls_require_fresh_body_bound_hmac(http_test_client) -> None:
    path = "/control/perception/discovery"
    assert http_test_client.post(path, content=b"{}").status_code == 401
    response = _signed(http_test_client, path, secret="wrong")
    assert response.status_code == 401
    assert (
        _signed(
            http_test_client,
            path,
            timestamp=str(int(time.time()) - 301),
        ).status_code
        == 401
    )
    assert (
        _signed(
            http_test_client,
            path,
            body=b'{"tampered":true}',
            signed_body=b"{}",
        ).status_code
        == 401
    )


def test_perception_control_queues_once_and_replay_is_rejected(
    http_test_client, daemon_settings_for_test
) -> None:
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    path = "/control/perception/discovery"
    nonce = uuid.uuid4().hex
    first = _signed(http_test_client, path, nonce=nonce)
    assert first.status_code == 202
    assert first.json()["status"] == "queued"
    assert _signed(http_test_client, path).json()["status"] == "already_queued"
    assert _signed(http_test_client, path, nonce=nonce).status_code == 401


def test_perception_control_refuses_disabled_and_escalated_component(
    http_test_client, daemon_settings_for_test
) -> None:
    assert _signed(http_test_client, "/control/perception/reconciliation").status_code == 409
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        con.executemany(
            "INSERT INTO neg_risk_incident_events("
            "incident_id,sequence,scope,kind,state,occurred_at_ms,evidence_json"
            ") VALUES('esc',?,'discovery','worker-failure',?,?,'{}')",
            ((1, "detected", 1), (2, "classified", 2), (3, "escalated", 3)),
        )
    response = _signed(http_test_client, "/control/perception/discovery")
    assert response.status_code == 409
    assert response.json() == {"status": "unavailable", "reason": "component-escalated"}


def test_perception_queue_contains_only_control_evidence(
    http_test_client, daemon_settings_for_test
):
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    assert _signed(http_test_client, "/control/perception/discovery").status_code == 202
    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT queued FROM neg_risk_operator_queue WHERE component='discovery'"
        ).fetchone() == (1,)
        assert con.execute("SELECT COUNT(*) FROM neg_risk_group_revisions").fetchone() == (0,)
        assert con.execute("SELECT COUNT(*) FROM neg_risk_incident_events").fetchone() == (0,)


def test_concurrent_controls_coalesce_without_bypassing_single_queue(
    http_test_client, daemon_settings_for_test
) -> None:
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(
            pool.map(
                lambda _: _signed(http_test_client, "/control/perception/discovery"),
                range(8),
            )
        )
    statuses = [response.status_code for response in responses]
    assert statuses.count(202) == 1
    assert statuses.count(200) == 7
