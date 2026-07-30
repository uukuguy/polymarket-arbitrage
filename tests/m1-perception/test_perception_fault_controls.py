from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import sqlite3
import threading
import time
import uuid
from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

from polyarb.config import Settings
from polyarb.http import perception_faults
from polyarb.perception.fault_authority import FaultAuthorityStore
from polyarb.perception.fault_control import FaultEventState, FaultRuntimeIdentity

ORDINARY_SECRET = "ordinary-control-secret"
FAULT_SECRET = "distinct-fault-control-secret"


@pytest.fixture
def make_http_test_client():
    from unittest.mock import MagicMock

    from starlette.testclient import TestClient

    from polyarb.http.app import create_app
    from polyarb.perception.store import OpportunityPerceptionStore
    from polyarb.storage.sqlite_store import SQLiteStore

    def make(settings):
        sqlite_store = SQLiteStore(settings.db_path)
        sqlite_store.init_schema()
        OpportunityPerceptionStore(settings.db_path).init_schema()
        return TestClient(
            create_app(
                scheduler=MagicMock(),
                sqlite_store=sqlite_store,
                settings=settings,
            ),
            raise_server_exceptions=True,
        )

    return make


def _client(tmp_path: Path, make_http_test_client, *, enabled: bool = True):
    settings = Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "parquet",
        cache_root=tmp_path / "cache",
        scan_shared_secret=SecretStr(ORDINARY_SECRET),
        upstream_fault_control_enabled=enabled,
        upstream_fault_control_secret=SecretStr(FAULT_SECRET if enabled else ""),
        supabase_url="",
        supabase_service_key=SecretStr(""),
        supabase_db_dsn=SecretStr("postgresql://test.invalid/test"),
        r2_endpoint="",
        r2_access_key_id=SecretStr(""),
        r2_secret_access_key=SecretStr(""),
    )
    client = make_http_test_client(settings)
    runtime = FaultRuntimeIdentity(
        component="candidate",
        release_id="a" * 40,
        machine_id="machine-1",
        boot_id=uuid.UUID("12345678-1234-4678-9234-567812345678"),
    )
    FaultAuthorityStore(settings.db_path).register_runtime_start(
        runtime, supervisor_run_id="run-1", attempt=1, started_at_ms=1_000
    )
    return client, runtime


def _body(identity: FaultRuntimeIdentity, **updates):
    value = {
        "fault_id": "fault-api-1",
        "kind": "clob-latency",
        "call_class": "clob-candidate-book-batch",
        "target_key": "group-1",
        "parameters": {"delay_ms": 10},
        "ttl_ms": 10_000,
        "runtime": {
            "component": identity.component,
            "release_id": identity.release_id,
            "machine_id": identity.machine_id,
            "boot_id": str(identity.boot_id),
        },
    }
    value.update(updates)
    return json.dumps(value, separators=(",", ":")).encode()


def _post(
    client,
    path: str,
    body: bytes,
    *,
    ordinary_nonce: str | None = None,
    fault_nonce: str | None = None,
    fault_secret: str = FAULT_SECRET,
    fault_timestamp: str | None = None,
    include_fault_signature: bool = True,
):
    timestamp = str(int(time.time()))
    ordinary_nonce = ordinary_nonce or uuid.uuid4().hex
    ordinary_canonical = b"\n".join(
        (timestamp.encode(), ordinary_nonce.encode(), b"POST", path.encode(), body)
    )
    headers = {
        "Content-Type": "application/json",
        "X-Perception-Timestamp": timestamp,
        "X-Perception-Nonce": ordinary_nonce,
        "X-Signature": hmac.new(
            ORDINARY_SECRET.encode(), ordinary_canonical, hashlib.sha256
        ).hexdigest(),
    }
    if include_fault_signature:
        fault_timestamp = fault_timestamp or timestamp
        fault_nonce = fault_nonce or uuid.uuid4().hex
        fault_canonical = b"\n".join(
            (
                b"polyarb-fault-v1",
                fault_timestamp.encode(),
                fault_nonce.encode(),
                b"POST",
                path.encode(),
                body,
            )
        )
        headers.update(
            {
                "X-Fault-Timestamp": fault_timestamp,
                "X-Fault-Nonce": fault_nonce,
                "X-Fault-Signature": hmac.new(
                    fault_secret.encode(), fault_canonical, hashlib.sha256
                ).hexdigest(),
            }
        )
    return client.post(path, content=body, headers=headers)


def test_enabled_fault_control_requires_a_distinct_nonempty_secret() -> None:
    with pytest.raises(ValidationError):
        Settings(
            upstream_fault_control_enabled=True,
            upstream_fault_control_secret=SecretStr(""),
        )


def test_arm_requires_enabled_and_second_fault_domain_hmac(
    tmp_path, make_http_test_client
) -> None:
    disabled, runtime = _client(tmp_path / "disabled", make_http_test_client, enabled=False)
    response = _post(disabled, "/control/perception/faults/arm", _body(runtime))
    assert response.status_code == 409
    assert response.json()["reason"] == "fault-control-disabled"

    enabled, runtime = _client(tmp_path / "enabled", make_http_test_client)
    response = _post(
        enabled,
        "/control/perception/faults/arm",
        _body(runtime),
        include_fault_signature=False,
    )
    assert response.status_code == 401
    with sqlite3.connect(enabled.app.state.sqlite_store.db_path) as con:
        assert con.execute("SELECT count(*) FROM neg_risk_fault_intents").fetchone() == (0,)


def test_arm_validates_body_runtime_replay_and_active_chain_before_accept(
    tmp_path, make_http_test_client
) -> None:
    client, runtime = _client(tmp_path, make_http_test_client)
    path = "/control/perception/faults/arm"
    for bad in (
        _body(runtime, mystery=True),
        _body(runtime, kind="unknown"),
        _body(runtime, target_key="https://secret.example"),
        _body(runtime, ttl_ms=999),
        _body(runtime, parameters={"delay_ms": 30001}),
    ):
        assert _post(client, path, bad).status_code == 400
    duplicate = _body(runtime)[:-1] + b',"ttl_ms":10000}'
    assert _post(client, path, duplicate).status_code == 400
    with sqlite3.connect(client.app.state.sqlite_store.db_path) as con:
        assert con.execute("SELECT count(*) FROM neg_risk_fault_intents").fetchone() == (0,)
        assert con.execute(
            "SELECT count(*) FROM neg_risk_fault_auth_nonces "
            "WHERE record_type='attempt' AND outcome='rejected'"
        ).fetchone() == (6,)
        database_text = "\n".join(
            str(value)
            for row in con.execute(
                "SELECT target_key,parameters_json FROM neg_risk_fault_intents"
            )
            for value in row
        )
    assert "https://secret.example" not in database_text
    assert FAULT_SECRET not in database_text

    body = _body(runtime)
    nonce = uuid.uuid4().hex
    first = _post(client, path, body, fault_nonce=nonce)
    assert first.status_code == 202
    assert first.json()["fault_id"] == "fault-api-1"
    assert set(first.json()) == {
        "status",
        "fault_id",
        "parameter_digest",
        "authorization_digest",
    }
    replay = _post(client, path, body, fault_nonce=nonce)
    assert replay.status_code == 401
    second = _post(
        client, path, _body(runtime, fault_id="fault-api-2"), fault_nonce=uuid.uuid4().hex
    )
    assert second.status_code == 409
    assert second.json()["reason"] == "fault-already-active"
    with sqlite3.connect(client.app.state.sqlite_store.db_path) as con:
        assert con.execute("SELECT count(*) FROM neg_risk_fault_intents").fetchone() == (1,)
        assert con.execute("SELECT count(*) FROM neg_risk_fault_events").fetchone() == (1,)


def test_arm_rejects_skew_malformed_nonce_oversize_and_wrong_runtime(
    tmp_path, make_http_test_client
) -> None:
    client, runtime = _client(tmp_path, make_http_test_client)
    path = "/control/perception/faults/arm"
    assert (
        _post(
            client,
            path,
            _body(runtime),
            fault_timestamp=str(int(time.time()) - 301),
        ).status_code
        == 401
    )
    assert _post(client, path, _body(runtime), fault_nonce="bad!").status_code == 401
    assert _post(client, path, b"x" * 65_537).status_code == 413
    wrong = _body(runtime, runtime={**json.loads(_body(runtime))["runtime"], "machine_id": "other"})
    response = _post(client, path, wrong)
    assert response.status_code == 409
    assert response.json()["reason"] == "runtime-mismatch"
    with sqlite3.connect(client.app.state.sqlite_store.db_path) as con:
        assert con.execute("SELECT count(*) FROM neg_risk_fault_intents").fetchone() == (0,)
        assert con.execute("SELECT count(*) FROM neg_risk_fault_events").fetchone() == (0,)


@pytest.mark.parametrize("offset_s", [-299, 299])
def test_signed_client_time_never_controls_intent_or_ttl(
    tmp_path, make_http_test_client, offset_s
) -> None:
    client, runtime = _client(tmp_path, make_http_test_client)
    before_ms = int(time.time() * 1_000)
    response = _post(
        client,
        "/control/perception/faults/arm",
        _body(runtime, ttl_ms=1_000),
        fault_timestamp=str(int(time.time()) + offset_s),
    )
    after_ms = int(time.time() * 1_000)
    assert response.status_code == 202
    with sqlite3.connect(client.app.state.sqlite_store.db_path) as con:
        accepted_at_ms, event_at_ms = con.execute(
            "SELECT i.accepted_at_ms,e.occurred_at_ms "
            "FROM neg_risk_fault_intents i JOIN neg_risk_fault_events e "
            "ON e.fault_id=i.fault_id WHERE e.state='authorized'"
        ).fetchone()
    assert before_ms <= accepted_at_ms <= after_ms
    assert event_at_ms == accepted_at_ms
    projection = FaultAuthorityStore(client.app.state.sqlite_store.db_path).project_fault(
        "fault-api-1", now_ms=accepted_at_ms + 1_000
    )
    assert projection.state is FaultEventState.EXPIRED
    cleanup_before_ms = int(time.time() * 1_000)
    cleanup = _post(
        client,
        "/control/perception/faults/cleanup",
        b'{"fault_id":"fault-api-1"}',
        fault_timestamp=str(int(time.time()) - offset_s),
    )
    cleanup_after_ms = int(time.time() * 1_000)
    assert cleanup.status_code == 202, cleanup.text
    with sqlite3.connect(client.app.state.sqlite_store.db_path) as con:
        action_at_ms = con.execute(
            "SELECT occurred_at_ms FROM neg_risk_fault_events "
            "WHERE action='cleanup-requested'"
        ).fetchone()[0]
    assert cleanup_before_ms <= action_at_ms <= cleanup_after_ms
    assert action_at_ms >= event_at_ms


def test_status_is_redacted_and_cleanup_never_fabricates_terminal(
    tmp_path, make_http_test_client
) -> None:
    client, runtime = _client(tmp_path, make_http_test_client)
    arm = _post(client, "/control/perception/faults/arm", _body(runtime))
    assert arm.status_code == 202
    status = client.get("/perception/faults/fault-api-1")
    assert status.status_code == 200
    payload = status.json()
    assert payload["complete_history"] is True
    assert payload["state"] == "authorized"
    encoded = status.text
    assert FAULT_SECRET not in encoded and "https://" not in encoded

    cleanup_body = b'{"fault_id":"fault-api-1"}'
    cleanup = _post(client, "/control/perception/faults/cleanup", cleanup_body)
    assert cleanup.status_code == 202, cleanup.text
    assert cleanup.json()["status"] == "cleanup-requested"
    again = _post(client, "/control/perception/faults/cleanup", cleanup_body)
    assert again.status_code == 202
    with sqlite3.connect(client.app.state.sqlite_store.db_path) as con:
        assert con.execute(
            "SELECT count(*) FROM neg_risk_fault_events WHERE action='cleanup-requested'"
        ).fetchone() == (1,)
        assert con.execute(
            "SELECT count(*) FROM neg_risk_fault_events WHERE state IN "
            "('cleaned','abandoned','expired')"
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT count(*) FROM neg_risk_fault_auth_nonces "
            "WHERE record_type='reservation'"
        ).fetchone() == (3,)


def test_cleanup_reports_producer_terminal_truth_instead_of_pending(
    tmp_path, make_http_test_client
) -> None:
    client, runtime = _client(tmp_path, make_http_test_client)
    assert _post(client, "/control/perception/faults/arm", _body(runtime)).status_code == 202
    authority = FaultAuthorityStore(client.app.state.sqlite_store.db_path)
    intent = authority.claim_pending(runtime, claimed_at_ms=int(time.time() * 1_000))
    assert intent is not None and intent.ownership_capability is not None
    now_ms = int(time.time() * 1_000)
    authority.append_event(
        "fault-api-1",
        FaultEventState.INJECTED,
        occurred_at_ms=now_ms,
        evidence={"call_id": "call-1"},
        ownership=intent.ownership_capability,
    )
    authority.append_event(
        "fault-api-1",
        FaultEventState.DETECTED,
        occurred_at_ms=now_ms,
        evidence={"incident_id": "incident-1"},
    )
    authority.append_event(
        "fault-api-1",
        FaultEventState.CONTAINED,
        occurred_at_ms=now_ms,
        evidence={"containment_id": "containment-1"},
    )
    authority.append_event(
        "fault-api-1",
        FaultEventState.CLEANED,
        occurred_at_ms=now_ms,
        evidence={"cleanup_id": "cleanup-1"},
        ownership=intent.ownership_capability,
    )
    response = _post(
        client,
        "/control/perception/faults/cleanup",
        b'{"fault_id":"fault-api-1"}',
    )
    assert response.status_code == 200
    assert response.json() == {
        "status": "already-terminal",
        "fault_id": "fault-api-1",
        "current_state": "cleaned",
    }


def test_fault_reads_run_off_event_loop_through_one_snapshot_method(
    tmp_path, make_http_test_client, monkeypatch
) -> None:
    client, runtime = _client(tmp_path, make_http_test_client)
    assert _post(client, "/control/perception/faults/arm", _body(runtime)).status_code == 202
    event_loop_threads: list[int] = []
    worker_threads: list[int] = []
    original_read = perception_faults._read_snapshot
    original_store_read = FaultAuthorityStore.read_snapshot

    async def observed_read(request, **kwargs):
        event_loop_threads.append(threading.get_ident())
        return await original_read(request, **kwargs)

    def observed_store_read(self, *args, **kwargs):
        worker_threads.append(threading.get_ident())
        return original_store_read(self, *args, **kwargs)

    monkeypatch.setattr(perception_faults, "_read_snapshot", observed_read)
    monkeypatch.setattr(FaultAuthorityStore, "read_snapshot", observed_store_read)
    assert client.get("/perception/faults/fault-api-1").status_code == 200
    assert client.get("/perception/faults/runtime?component=candidate").status_code == 200
    assert len(event_loop_threads) == len(worker_threads) == 2
    assert all(
        event_thread != worker_thread
        for event_thread, worker_thread in zip(event_loop_threads, worker_threads)
    )


def test_timeout_response_cannot_race_a_late_intent_commit(
    tmp_path, make_http_test_client, monkeypatch
) -> None:
    client, runtime = _client(tmp_path, make_http_test_client)
    entered = threading.Event()
    release = threading.Event()
    response_done = threading.Event()
    responses: list[object] = []
    original = FaultAuthorityStore.accept_intent

    def blocked(self, *args, **kwargs):
        entered.set()
        assert release.wait(timeout=2)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(FaultAuthorityStore, "accept_intent", blocked)

    def send() -> None:
        responses.append(
            _post(client, "/control/perception/faults/arm", _body(runtime))
        )
        response_done.set()

    request_thread = threading.Thread(target=send)
    request_thread.start()
    assert entered.wait(timeout=1)
    assert response_done.wait(timeout=1.0)
    assert responses[0].status_code == 409
    release.set()
    request_thread.join(timeout=2)
    time.sleep(0.05)

    with sqlite3.connect(client.app.state.sqlite_store.db_path) as con:
        assert con.execute("SELECT count(*) FROM neg_risk_fault_auth_nonces").fetchone() == (0,)
        assert con.execute("SELECT count(*) FROM neg_risk_fault_intents").fetchone() == (0,)
        assert con.execute("SELECT count(*) FROM neg_risk_fault_events").fetchone() == (0,)


def test_cleanup_slow_snapshot_uses_one_200ms_deadline(
    tmp_path, make_http_test_client, monkeypatch
) -> None:
    client, runtime = _client(tmp_path, make_http_test_client)
    assert _post(client, "/control/perception/faults/arm", _body(runtime)).status_code == 202
    original = FaultAuthorityStore.read_snapshot

    def slow_read(self, *args, **kwargs):
        time.sleep(0.5)
        return original(self, *args, **kwargs)

    monkeypatch.setattr(FaultAuthorityStore, "read_snapshot", slow_read)
    started = time.monotonic()
    response = _post(
        client,
        "/control/perception/faults/cleanup",
        b'{"fault_id":"fault-api-1"}',
    )
    elapsed = time.monotonic() - started

    assert elapsed < 0.35
    assert response.status_code == 409
    assert response.json() == {
        "status": "unavailable",
        "reason": "fault-control-store-unavailable",
    }


def test_fault_worker_bridge_has_bounded_threads_zero_queue_and_recovers() -> None:
    for _ in range(100):
        if not any(
            thread.name == "fault-authority" for thread in threading.enumerate()
        ):
            break
        time.sleep(0.01)
    release = threading.Event()
    lock = threading.Lock()
    started = 0

    def blocked() -> str:
        nonlocal started
        with lock:
            started += 1
        assert release.wait(timeout=2)
        return "done"

    async def exercise() -> None:
        tasks = [
            asyncio.create_task(perception_faults._run_blocking(blocked))
            for _ in range(5)
        ]
        for _ in range(50):
            with lock:
                if started >= perception_faults._FAULT_WORKER_LIMIT:
                    break
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        with lock:
            assert started == perception_faults._FAULT_WORKER_LIMIT
        release.set()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        assert results.count("done") == perception_faults._FAULT_WORKER_LIMIT
        assert sum(isinstance(value, TimeoutError) for value in results) == 1
        assert await perception_faults._run_blocking(lambda: "recovered") == "recovered"

    asyncio.run(exercise())
    for _ in range(50):
        if not any(
            thread.name == "fault-authority" for thread in threading.enumerate()
        ):
            break
        time.sleep(0.01)
    assert not any(
        thread.name == "fault-authority" for thread in threading.enumerate()
    )


def test_runtime_read_is_bounded_and_missing_evidence_is_unavailable(
    tmp_path, make_http_test_client
) -> None:
    client, runtime = _client(tmp_path, make_http_test_client)
    response = client.get("/perception/faults/runtime?component=candidate")
    assert response.status_code == 200
    assert response.json()["runtime"]["boot_id"] == str(runtime.boot_id)
    missing = client.get("/perception/faults/missing")
    assert missing.status_code == 503
    assert missing.json()["status"] == "unavailable"
