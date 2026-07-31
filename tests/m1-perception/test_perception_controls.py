from __future__ import annotations

import asyncio
import hashlib
import hmac
import sqlite3
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from starlette.requests import Request

from polyarb.perception import store as perception_store
from polyarb.perception.discovery import DiscoveryRunner
from polyarb.perception.incidents import IncidentManager
from polyarb.perception.reconciliation import ReconciliationRunner
from polyarb.perception.store import OpportunityPerceptionStore

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


def test_perception_control_rejects_oversized_body_before_auth_persistence(
    http_test_client,
) -> None:
    response = _signed(
        http_test_client,
        "/control/perception/discovery",
        body=b"x" * 65_537,
    )
    assert response.status_code == 413
    with sqlite3.connect(http_test_client.app.state.sqlite_store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_operator_auth_nonces"
        ).fetchone() == (0,)


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
    manager = IncidentManager(
        OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    )
    incident = manager.detect("discovery", "worker-failure", {})
    manager.transition(incident.id, "classified", {})
    manager.transition(incident.id, "escalated", {})
    response = _signed(http_test_client, "/control/perception/discovery")
    assert response.status_code == 409
    assert response.json() == {
        "status": "unavailable",
        "reason": "component-incident-active",
    }


def test_perception_control_refuses_every_active_component_incident(
    http_test_client, daemon_settings_for_test
) -> None:
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    db_path = http_test_client.app.state.sqlite_store.db_path
    IncidentManager(
        OpportunityPerceptionStore(http_test_client.app.state.sqlite_store.db_path)
    ).detect("discovery", "worker-failure", {})
    response = _signed(http_test_client, "/control/perception/discovery")
    assert response.status_code == 409
    assert response.json()["reason"] == "component-incident-active"
    with sqlite3.connect(db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM neg_risk_operator_queue").fetchone() == (0,)


def test_component_control_refuses_active_incident_after_suffix_compaction(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    manager = IncidentManager(store, clock_ms=lambda: 1_000)
    active = manager.detect("discovery", "worker-failure", {})
    for index in range(512):
        manager.detect(
            f"operator:compaction-{index}",
            "manual-investigation",
            {},
        )

    with store._connect() as con:
        assert con.execute(
            "SELECT 1 FROM neg_risk_incident_open_authority WHERE incident_id=?",
            (active.id,),
        ).fetchone() is not None
        assert con.execute(
            "SELECT 1 FROM neg_risk_incident_events WHERE incident_id=?",
            (active.id,),
        ).fetchone() is None
        with pytest.raises(RuntimeError, match="component-incident-active"):
            store._validate_component_control_permission(
                con,
                "discovery",
                now_ms=2_000,
            )


def test_operator_queue_deadline_rolls_back_before_return(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db", busy_timeout_ms=250)
    store.init_schema()

    def cross_deadline() -> None:
        time.sleep(0.06)

    auth_deadline = time.monotonic() + 0.5
    store.accept_operator_auth(
        nonce="deadline-nonce",
        request_method="POST",
        request_path="/control/perception/discovery",
        request_timestamp_s=1,
        body_hash=hashlib.sha256(b"{}").hexdigest(),
        accepted_at_ms=1_000,
        deadline_monotonic=auth_deadline,
    )
    deadline = time.monotonic() + 0.03
    try:
        store.queue_operator_wakeup(
            "discovery",
            request_nonce="deadline-nonce",
            occurred_at_ms=1,
            deadline_monotonic=deadline,
            _before_commit=cross_deadline,
        )
    except TimeoutError:
        pass
    else:
        raise AssertionError("deadline crossing must fail closed")
    with sqlite3.connect(store.db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM neg_risk_operator_queue").fetchone() == (0,)
        assert con.execute("SELECT COUNT(*) FROM neg_risk_operator_queue_receipts").fetchone() == (
            0,
        )


def test_operator_auth_and_queue_history_have_fail_closed_capacity_bounds(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db", busy_timeout_ms=250)
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        con.executemany(
            "INSERT INTO neg_risk_operator_auth_nonces("
            "nonce,request_method,request_path,request_timestamp_s,body_hash,"
            "accepted_at_ms,auth_hash) VALUES(?, 'POST',"
            "'/control/perception/discovery',1,?,1000,'invalid')",
            (
                (f"nonce-{index}", "0" * 64)
                for index in range(10_001)
            ),
        )
    with sqlite3.connect(store.db_path) as con, pytest.raises(
        ValueError,
        match="operator-auth-history-capacity-exceeded",
    ):
        con.row_factory = sqlite3.Row
        store._validated_operator_auth(con)


def _replace_with_task6_legacy_operator_schema(
    db_path,
    *,
    receipt_action: str = "queued",
) -> None:
    with sqlite3.connect(db_path) as con:
        con.executescript(
            """
            DROP TABLE neg_risk_operator_queue_receipts;
            DROP TABLE neg_risk_operator_queue;
            DROP TABLE neg_risk_operator_auth_nonces;
            CREATE TABLE neg_risk_operator_auth_nonces (
              nonce TEXT PRIMARY KEY,
              request_path TEXT NOT NULL,
              request_timestamp_s INTEGER NOT NULL,
              accepted_at_ms INTEGER NOT NULL
            );
            CREATE TABLE neg_risk_operator_queue (
              component TEXT PRIMARY KEY,
              queued INTEGER NOT NULL,
              queued_at_ms INTEGER,
              consumed_at_ms INTEGER,
              request_nonce TEXT
            );
            CREATE TABLE neg_risk_operator_queue_receipts (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              component TEXT NOT NULL,
              action TEXT NOT NULL,
              occurred_at_ms INTEGER NOT NULL,
              request_nonce TEXT,
              UNIQUE(component,action,request_nonce)
            );
            INSERT INTO neg_risk_operator_auth_nonces
              VALUES('legacy-nonce','/control/perception/discovery',1,1000);
            INSERT INTO neg_risk_operator_queue
              VALUES('discovery',1,1000,NULL,'legacy-nonce');
            """
        )
        con.execute(
            "INSERT INTO neg_risk_operator_queue_receipts("
            "component,action,occurred_at_ms,request_nonce) VALUES(?,?,?,?)",
            ("discovery", receipt_action, 1_000, "legacy-nonce"),
        )


def test_task6_legacy_operator_queue_migrates_atomically_and_idempotently(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    _replace_with_task6_legacy_operator_schema(store.db_path)

    store.init_schema()
    store.init_schema()

    assert store.pending_operator_wakeup(
        "discovery",
        now_ms=1_000,
    ) == "legacy-nonce"
    with sqlite3.connect(store.db_path) as con:
        auth = con.execute(
            "SELECT request_method,body_hash,auth_hash "
            "FROM neg_risk_operator_auth_nonces"
        ).fetchone()
        receipt = con.execute(
            "SELECT sequence,auth_nonce,previous_hash,receipt_hash "
            "FROM neg_risk_operator_queue_receipts"
        ).fetchone()
        queue = con.execute(
            "SELECT last_sequence,last_receipt_hash "
            "FROM neg_risk_operator_queue WHERE component='discovery'"
        ).fetchone()
    assert auth[0] == "POST"
    assert auth[1] == hashlib.sha256(b"{}").hexdigest()
    assert all(value is not None for value in (*auth, receipt[0], receipt[1], receipt[3]))
    assert receipt[0] == queue[0] == 1
    assert receipt[3] == queue[1]


def test_v1_hash_chain_with_multiple_receipts_upgrades_without_rewriting_history(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db", busy_timeout_ms=250)
    store.init_schema()
    for nonce, accepted_at_ms in (("first", 1_000), ("second", 2_000)):
        store.accept_operator_auth(
            nonce=nonce,
            request_method="POST",
            request_path="/control/perception/discovery",
            request_timestamp_s=accepted_at_ms // 1_000,
            body_hash=hashlib.sha256(b"{}").hexdigest(),
            accepted_at_ms=accepted_at_ms,
            deadline_monotonic=time.monotonic() + 1,
        )
        store.queue_operator_wakeup(
            "discovery",
            request_nonce=nonce,
            occurred_at_ms=accepted_at_ms,
            deadline_monotonic=time.monotonic() + 1,
        )
    with sqlite3.connect(store.db_path) as con:
        con.row_factory = sqlite3.Row
        previous_hash = None
        for row in con.execute(
            "SELECT * FROM neg_risk_operator_queue_receipts ORDER BY sequence"
        ).fetchall():
            receipt_hash = perception_store.operator_queue_receipt_hash(
                component=row["component"],
                sequence=row["sequence"],
                action=row["action"],
                occurred_at_ms=row["occurred_at_ms"],
                auth_nonce=row["auth_nonce"],
                previous_hash=previous_hash,
            )
            con.execute(
                "UPDATE neg_risk_operator_queue_receipts "
                "SET previous_hash=?,receipt_hash=? WHERE id=?",
                (previous_hash, receipt_hash, row["id"]),
            )
            previous_hash = receipt_hash
        con.execute(
            "UPDATE neg_risk_operator_queue SET last_receipt_hash=?",
            (previous_hash,),
        )
        con.execute(
            "ALTER TABLE neg_risk_operator_queue_receipts "
            "DROP COLUMN auth_receipt_hash"
        )
        con.execute(
            "ALTER TABLE neg_risk_operator_queue DROP COLUMN request_auth_hash"
        )

    store.init_schema()
    assert store.pending_operator_wakeup(
        "discovery",
        now_ms=2_000,
    ) == "first"


def test_invalid_task6_legacy_operator_queue_rolls_back_entire_migration(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    _replace_with_task6_legacy_operator_schema(
        store.db_path,
        receipt_action="consumed",
    )

    with pytest.raises(ValueError, match="invalid-legacy-operator-queue"):
        store.init_schema()

    with sqlite3.connect(store.db_path) as con:
        columns = {
            row[1]
            for row in con.execute(
                "PRAGMA table_info(neg_risk_operator_auth_nonces)"
            )
        }
    assert "request_method" not in columns


def test_expired_auth_nonce_is_pruned_without_losing_queue_proof(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db", busy_timeout_ms=250)
    store.init_schema()
    store.accept_operator_auth(
        nonce="old-nonce",
        request_method="POST",
        request_path="/control/perception/discovery",
        request_timestamp_s=1,
        body_hash=hashlib.sha256(b"{}").hexdigest(),
        accepted_at_ms=1_000,
        deadline_monotonic=time.monotonic() + 1,
    )
    store.queue_operator_wakeup(
        "discovery",
        request_nonce="old-nonce",
        occurred_at_ms=1_000,
        deadline_monotonic=time.monotonic() + 1,
    )
    store.accept_operator_auth(
        nonce="new-nonce",
        request_method="POST",
        request_path="/control/perception/discovery",
        request_timestamp_s=302,
        body_hash=hashlib.sha256(b"{}").hexdigest(),
        accepted_at_ms=302_000,
        deadline_monotonic=time.monotonic() + 1,
    )

    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT nonce FROM neg_risk_operator_auth_nonces ORDER BY nonce"
        ).fetchall() == [("new-nonce",)]
    assert store.pending_operator_wakeup(
        "discovery",
        now_ms=302_000,
    ) == "old-nonce"


def test_control_auth_and_queue_share_one_absolute_response_deadline(
    http_test_client,
    daemon_settings_for_test,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    db_path = http_test_client.app.state.sqlite_store.db_path
    now_s = int(time.time())
    accepted_at_ms = now_s * 1_000
    body_hash = hashlib.sha256(b"{}").hexdigest()
    rows = []
    for index in range(1_800):
        nonce = f"history-{index}"
        auth_hash = perception_store.operator_auth_receipt_hash(
            nonce=nonce,
            request_method="POST",
            request_path="/control/perception/discovery",
            request_timestamp_s=now_s,
            body_hash=body_hash,
            accepted_at_ms=accepted_at_ms,
        )
        rows.append(
            (
                nonce,
                "POST",
                "/control/perception/discovery",
                now_s,
                body_hash,
                accepted_at_ms,
                auth_hash,
            )
        )
    with sqlite3.connect(db_path) as con:
        con.executemany(
            "INSERT INTO neg_risk_operator_auth_nonces VALUES(?,?,?,?,?,?,?)",
            rows,
        )

    original_hash = perception_store.operator_auth_receipt_hash

    def slow_hash(**kwargs):
        time.sleep(0.0002)
        return original_hash(**kwargs)

    monkeypatch.setattr(perception_store, "operator_auth_receipt_hash", slow_hash)
    started = time.monotonic()
    response = _signed(http_test_client, "/control/perception/discovery")
    elapsed = time.monotonic() - started

    assert response.status_code == 409
    assert elapsed <= 1.1
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_operator_queue"
        ).fetchone() == (0,)


def _seed_large_operator_queue(store: OpportunityPerceptionStore, count: int) -> str:
    auth_hash = "a" * 64
    previous_hash = None
    rows = []
    for sequence in range(1, count + 1):
        nonce = "queued-nonce" if sequence == 1 else f"coalesced-{sequence}"
        action = "queued" if sequence == 1 else "coalesced"
        receipt_hash = perception_store.operator_queue_receipt_hash(
            component="discovery",
            sequence=sequence,
            action=action,
            occurred_at_ms=sequence,
            auth_nonce=nonce,
            auth_receipt_hash=auth_hash,
            previous_hash=previous_hash,
        )
        rows.append(
            (
                "discovery",
                sequence,
                action,
                sequence,
                nonce,
                auth_hash,
                previous_hash,
                receipt_hash,
            )
        )
        previous_hash = receipt_hash
    with sqlite3.connect(store.db_path) as con:
        con.executemany(
            "INSERT INTO neg_risk_operator_queue_receipts("
            "component,sequence,action,occurred_at_ms,auth_nonce,"
            "auth_receipt_hash,previous_hash,receipt_hash) "
            "VALUES(?,?,?,?,?,?,?,?)",
            rows,
        )
        con.execute(
            "INSERT INTO neg_risk_operator_queue("
            "component,queued,queued_at_ms,consumed_at_ms,request_nonce,"
            "request_auth_hash,last_sequence,last_receipt_hash"
            ") VALUES('discovery',1,1,NULL,'queued-nonce',?,?,?)",
            (auth_hash, count, previous_hash),
        )
    return previous_hash


def test_queue_history_rolls_checkpoint_and_consumes_across_boundary(
    tmp_path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db", busy_timeout_ms=250)
    store.init_schema()
    _seed_large_operator_queue(store, 10_001)

    assert store.pending_operator_wakeup(
        "discovery",
        now_ms=10_001,
    ) == "queued-nonce"
    with sqlite3.connect(store.db_path) as con:
        checkpoint = con.execute(
            "SELECT through_sequence,queued,request_nonce "
            "FROM neg_risk_operator_queue_checkpoints "
            "WHERE component='discovery'"
        ).fetchone()
        suffix_count = con.execute(
            "SELECT COUNT(*) FROM neg_risk_operator_queue_receipts "
            "WHERE component='discovery'"
        ).fetchone()[0]
    assert checkpoint == (9_001, 1, "queued-nonce")
    assert suffix_count == 1_000
    assert store.consume_operator_wakeup(
        "discovery",
        occurred_at_ms=10_002,
        expected_nonce="queued-nonce",
    )
    assert not store.consume_operator_wakeup(
        "discovery",
        occurred_at_ms=10_003,
        expected_nonce="queued-nonce",
    )


def test_checkpoint_tampering_fails_closed(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db", busy_timeout_ms=250)
    store.init_schema()
    _seed_large_operator_queue(store, 10_001)
    assert store.pending_operator_wakeup("discovery", now_ms=10_001)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "UPDATE neg_risk_operator_queue_checkpoints "
            "SET checkpoint_hash='forged' WHERE component='discovery'"
        )
    with pytest.raises(ValueError, match="invalid-operator-queue-checkpoint"):
        store.pending_operator_wakeup("discovery", now_ms=10_002)


def test_checkpoint_write_and_prefix_delete_are_atomic(tmp_path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db", busy_timeout_ms=250)
    store.init_schema()
    _seed_large_operator_queue(store, 10_001)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "CREATE TRIGGER reject_operator_prefix_delete "
            "BEFORE DELETE ON neg_risk_operator_queue_receipts "
            "BEGIN SELECT RAISE(ABORT,'injected-checkpoint-crash'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="injected-checkpoint-crash"):
        store.pending_operator_wakeup("discovery", now_ms=10_001)
    with sqlite3.connect(store.db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_operator_queue_checkpoints"
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_operator_queue_receipts"
        ).fetchone() == (10_001,)


@pytest.mark.asyncio
async def test_slow_drip_body_obeys_control_absolute_deadline() -> None:
    chunks = [
        {"type": "http.request", "body": b"{", "more_body": True},
        {"type": "http.request", "body": b"}", "more_body": False},
    ]

    async def receive():
        await asyncio.sleep(0.5)
        return chunks.pop(0)

    request = Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/control/perception/discovery",
            "raw_path": b"/control/perception/discovery",
            "query_string": b"",
            "headers": [(b"x-signature", b"sha256=invalid")],
            "client": ("test", 1),
            "server": ("test", 80),
        },
        receive,
    )
    started = time.monotonic()
    response = await __import__(
        "polyarb.http.control",
        fromlist=["control_auth_middleware"],
    ).control_auth_middleware(
        request,
        lambda _request: None,
        secret=_SECRET,
    )
    elapsed = time.monotonic() - started

    assert response.status_code == 408
    assert elapsed <= 1.05


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


def test_corrupt_auth_or_queue_history_fails_closed(
    http_test_client, daemon_settings_for_test
) -> None:
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    path = "/control/perception/discovery"
    assert _signed(http_test_client, path).status_code == 202
    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        con.execute(
            "UPDATE neg_risk_operator_queue_receipts SET receipt_hash='forged'"
        )
    assert _signed(http_test_client, path).status_code == 409


def test_corrupt_auth_body_binding_fails_closed(
    http_test_client, daemon_settings_for_test
) -> None:
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    path = "/control/perception/discovery"
    assert _signed(http_test_client, path).status_code == 202
    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        con.execute(
            "UPDATE neg_risk_operator_auth_nonces SET body_hash=?",
            ("0" * 64,),
        )
    assert _signed(http_test_client, path).status_code == 409


def test_active_incident_prevents_consume_and_preserves_queue(
    http_test_client, daemon_settings_for_test
) -> None:
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    assert _signed(
        http_test_client, "/control/perception/discovery"
    ).status_code == 202
    db_path = http_test_client.app.state.sqlite_store.db_path
    store = OpportunityPerceptionStore(db_path, busy_timeout_ms=250)
    IncidentManager(store).detect("discovery", "worker-failure", {})
    assert not store.consume_operator_wakeup(
        "discovery", occurred_at_ms=int(time.time() * 1_000)
    )
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT queued FROM neg_risk_operator_queue "
            "WHERE component='discovery'"
        ).fetchone() == (1,)
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_operator_queue_receipts "
            "WHERE action='consumed'"
        ).fetchone() == (0,)


def test_peek_is_crash_safe_and_terminal_consume_is_exactly_once(
    http_test_client, daemon_settings_for_test
) -> None:
    http_test_client.app.state.settings = daemon_settings_for_test.model_copy(
        update={"opportunity_discovery_enabled": True}
    )
    assert _signed(
        http_test_client, "/control/perception/discovery"
    ).status_code == 202
    store = OpportunityPerceptionStore(
        http_test_client.app.state.sqlite_store.db_path,
        busy_timeout_ms=250,
    )
    now_ms = int(time.time() * 1_000)
    nonce = store.pending_operator_wakeup("discovery", now_ms=now_ms)
    assert nonce is not None
    # Simulated crash before producer terminalization: a second process sees
    # the same durable request because peek never consumes it.
    assert store.pending_operator_wakeup("discovery", now_ms=now_ms) == nonce
    assert store.consume_operator_wakeup(
        "discovery",
        occurred_at_ms=now_ms,
        expected_nonce=nonce,
    )
    assert not store.consume_operator_wakeup(
        "discovery",
        occurred_at_ms=now_ms,
        expected_nonce=nonce,
    )


class _RunnerStore:
    def __init__(self, nonce: str) -> None:
        self.nonce = nonce
        self.consumed: list[tuple[str, str | None]] = []

    def pending_operator_wakeup(self, component, **_kwargs):
        return self.nonce

    def consume_operator_wakeup(self, component, *, expected_nonce=None, **_kwargs):
        self.consumed.append((component, expected_nonce))
        return True

    def record_producer_heartbeat(self, *_args, **_kwargs):
        return None


class _RunnerGamma:
    async def aclose(self) -> None:
        return None


@pytest.mark.asyncio
async def test_discovery_runner_preserves_nonce_after_failed_or_yielded_attempt() -> None:
    for result in (RuntimeError("failed"), SimpleNamespace(finished_at_ms=1, yielded=True)):
        stop = __import__("asyncio").Event()
        store = _RunnerStore("discovery-nonce")

        class Worker:
            _require_resource_decision = False

            async def run_batch(self):
                stop.set()
                if isinstance(result, BaseException):
                    raise result
                return result

        await DiscoveryRunner(
            worker=Worker(),
            gamma=_RunnerGamma(),
            interval_s=1,
            store=store,
        ).run(stop)
        assert store.consumed == []


@pytest.mark.asyncio
async def test_discovery_runner_consumes_exact_nonce_after_successful_checkpoint() -> None:
    stop = __import__("asyncio").Event()
    store = _RunnerStore("discovery-nonce")

    class Worker:
        _require_resource_decision = False

        async def run_batch(self):
            stop.set()
            return SimpleNamespace(
                finished_at_ms=1,
                yielded=False,
                batch_id=None,
            )

    await DiscoveryRunner(
        worker=Worker(),
        gamma=_RunnerGamma(),
        interval_s=1,
        store=store,
    ).run(stop)
    assert store.consumed == [("discovery", "discovery-nonce")]


@pytest.mark.asyncio
async def test_reconciliation_runner_preserves_nonce_after_failed_window() -> None:
    stop = __import__("asyncio").Event()
    store = _RunnerStore("reconciliation-nonce")

    class Worker:
        async def run_batch(self):
            stop.set()
            return SimpleNamespace(finished_at_ms=1, failed=True)

    await ReconciliationRunner(
        worker=Worker(),
        gamma=_RunnerGamma(),
        interval_s=1,
        store=store,
    ).run(stop)
    assert store.consumed == []


@pytest.mark.asyncio
async def test_reconciliation_runner_consumes_exact_nonce_after_successful_checkpoint() -> None:
    stop = __import__("asyncio").Event()
    store = _RunnerStore("reconciliation-nonce")

    class Worker:
        async def run_batch(self):
            stop.set()
            return SimpleNamespace(finished_at_ms=1, failed=False)

    await ReconciliationRunner(
        worker=Worker(),
        gamma=_RunnerGamma(),
        interval_s=1,
        store=store,
    ).run(stop)
    assert store.consumed == [("reconciliation", "reconciliation-nonce")]
