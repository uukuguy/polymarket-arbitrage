"""Contract tests for the private watchdog event writer boundary."""

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from polyarb.control_plane.alert_delivery import render_runtime_incident_message
from polyarb.control_plane.runtime_event_writer import append_runtime_event, healthz


def _runtime_transition_payload(
    *,
    transition: str = "detected",
    occurred_at: str = "2030-01-01T00:01:00+00:00",
) -> dict[str, object]:
    return {
        "schema_version": "m1-runtime-incident-transition-v1",
        "transition": transition,
        "incident_id": "runtime-watchdog-incident-a",
        "incident_key": "runtime-watchdog:independent-runtime-watchdog",
        "component": "runtime-watchdog",
        "source": "independent-runtime-watchdog",
        "job_key": "quote:batch:42",
        "stage": "quote-fetch",
        "reason": "control-api:TimeoutError",
        "action": "restart-machine",
        "qualification_impact": "invalidated",
        "dashboard_url": "https://dashboard.example/control-plane",
        "occurred_at": occurred_at,
    }


def test_writer_rejects_unauthenticated_and_unbounded_event_detail(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    app = Starlette(
        routes=[
            Route("/healthz", healthz),
            Route("/runtime-events", append_runtime_event, methods=["POST"]),
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        assert client.get("/healthz").json() == {"status": "ok"}
        assert client.post("/runtime-events", json={}).status_code == 401
        invalid = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "a" * 64},
            json={
                "kind": "detected",
                "failures": ["contains a space"],
                "occurred_at": "2026-08-18T15:00:00+00:00",
            },
        )
        invalid_source = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "b" * 64},
            json={
                "kind": "detected",
                "failures": ["machine:alert:stopped"],
                "source": "contains a space",
                "occurred_at": "2026-08-18T15:00:00+00:00",
            },
        )
    assert invalid.status_code == 400
    assert invalid_source.status_code == 400


def test_writer_returns_existing_receipt_for_an_idempotent_retry(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    from polyarb.control_plane import runtime_event_writer

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.parameters: list[object] = []
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, params=()):
            self.calls.append(sql)
            if not sql.startswith("SET LOCAL"):
                self.parameters.append(params)
        def fetchone(self): return {"incident_event_id": "event-existing"}

    cursor = Cursor()

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def cursor(self, **_kwargs): return cursor

    connect_calls: list[dict[str, object]] = []

    def connect(_dsn: str, **kwargs: object) -> Connection:
        connect_calls.append(kwargs)
        return Connection()

    monkeypatch.setattr(runtime_event_writer.psycopg, "connect", connect)
    app = Starlette(
        routes=[
            Route("/runtime-events", runtime_event_writer.append_runtime_event, methods=["POST"])
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        response = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "a" * 64},
            json={
                "kind": "detected",
                "failures": ["control-api:timeout"],
                "occurred_at": "2026-08-18T15:00:00+00:00",
            },
        )
    assert response.status_code == 201
    assert response.json() == {"status": "duplicate", "incident_event_id": "event-existing"}
    assert cursor.parameters[0] == ("runtime:" + "a" * 64,)
    assert cursor.calls[:2] == [
        "SET LOCAL statement_timeout = '5000ms'",
        "SET LOCAL lock_timeout = '1000ms'",
    ]
    assert connect_calls == [{"connect_timeout": 5}]


def test_writer_records_detected_event_against_conflict_returned_incident(monkeypatch) -> None:
    """The unique incident row, not a locally generated UUID, owns the event."""
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    from polyarb.control_plane import runtime_event_writer

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.parameters: list[object] = []
            self.outbox_payloads: list[dict[str, object]] = []

        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, params=()):
            self.calls.append(sql)
            if not sql.startswith("SET LOCAL"):
                self.parameters.append(params)
            if "INSERT INTO m1_alert_outbox" in sql:
                self.outbox_payloads.append(params[3])

        def fetchone(self):
            query_count = sum(not call.startswith("SET LOCAL") for call in self.calls)
            if query_count == 1:
                return None
            if query_count == 2:
                return None
            return {"incident_key": "persisted-incident"}

    cursor = Cursor()
    monkeypatch.setattr(runtime_event_writer, "Jsonb", lambda value: value)

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def cursor(self, **_kwargs): return cursor

    connect_calls: list[dict[str, object]] = []

    def connect(_dsn: str, **kwargs: object) -> Connection:
        connect_calls.append(kwargs)
        return Connection()

    monkeypatch.setattr(runtime_event_writer.psycopg, "connect", connect)
    app = Starlette(
        routes=[
            Route("/runtime-events", runtime_event_writer.append_runtime_event, methods=["POST"])
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        response = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "b" * 64},
            json={
                "kind": "detected",
                "failures": ["control-api:timeout"],
                "source": "cloudflare-watchdog-supervisor",
                "occurred_at": "2026-08-18T15:00:00+00:00",
            },
        )
    assert response.status_code == 201
    assert response.json()["incident_key"] == "persisted-incident"
    assert cursor.parameters[1] == ("runtime-watchdog:cloudflare-watchdog-supervisor",)
    assert cursor.calls[:2] == [
        "SET LOCAL statement_timeout = '5000ms'",
        "SET LOCAL lock_timeout = '1000ms'",
    ]
    assert any("RETURNING incident_key" in call for call in cursor.calls)
    event_parameters = next(
        params
        for params in cursor.parameters
        if isinstance(params, tuple) and len(params) > 2 and params[2] == "detected"
    )
    assert event_parameters[3] == {
        "failures": ["control-api:timeout"],
        "source": "cloudflare-watchdog-supervisor",
    }
    assert len(cursor.outbox_payloads) == 2
    assert {
        params[2]
        for params in cursor.parameters
        if isinstance(params, tuple)
        and len(params) > 2
        and params[2] in {"dashboard", "telegram"}
    } == {
        "dashboard",
        "telegram",
    }
    assert cursor.outbox_payloads[0]["schema_version"] == "m1-runtime-incident-transition-v1"
    assert cursor.outbox_payloads[0]["transition"] == "detected"
    assert "DETECTED" in render_runtime_incident_message(cursor.outbox_payloads[0])
    assert connect_calls == [{"connect_timeout": 5}]


def test_writer_accepts_initial_recovery_as_noop(monkeypatch) -> None:
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    from polyarb.control_plane import runtime_event_writer

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, _params=()): self.calls.append(sql)
        def fetchone(self): return None

    cursor = Cursor()

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def cursor(self, **_kwargs): return cursor

    connect_calls: list[dict[str, object]] = []

    def connect(_dsn: str, **kwargs: object) -> Connection:
        connect_calls.append(kwargs)
        return Connection()

    monkeypatch.setattr(runtime_event_writer.psycopg, "connect", connect)
    app = Starlette(
        routes=[
            Route("/runtime-events", runtime_event_writer.append_runtime_event, methods=["POST"])
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        response = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "c" * 64},
            json={"kind": "recovered", "failures": [], "occurred_at": "2026-08-18T15:00:00+00:00"},
        )
    assert response.status_code == 201
    assert response.json() == {"status": "noop"}
    assert cursor.calls[:2] == [
        "SET LOCAL statement_timeout = '5000ms'",
        "SET LOCAL lock_timeout = '1000ms'",
    ]
    assert connect_calls == [{"connect_timeout": 5}]


def test_runtime_transition_writer_suppresses_restart_duplicate_from_open_incident(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    from polyarb.control_plane import runtime_event_writer

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, _params=()): self.calls.append(sql)
        def fetchone(self):
            query_count = sum(not call.startswith("SET LOCAL") for call in self.calls)
            if query_count == 1:
                return None
            if query_count == 2:
                return {
                    "incident_key": "runtime-incident-a",
                    "state": "open",
                    "opened_at": "2030-01-01T00:00:00+00:00",
                }
            if query_count == 3:
                return {"kind": "detected", "occurred_at": "2030-01-01T00:00:00+00:00"}
            if query_count == 4:
                return {"kind": "detected", "occurred_at": "2030-01-01T00:00:00+00:00"}
            raise AssertionError("duplicate unhealthy observation must not write another event")

    cursor = Cursor()

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def cursor(self, **_kwargs): return cursor

    monkeypatch.setattr(
        runtime_event_writer.psycopg,
        "connect",
        lambda *_args, **_kwargs: Connection(),
    )
    app = Starlette(
        routes=[
            Route("/runtime-events", runtime_event_writer.append_runtime_event, methods=["POST"])
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        response = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "d" * 64},
            json=_runtime_transition_payload(),
        )

    assert response.status_code == 201
    assert response.json() == {"status": "noop"}


def test_runtime_transition_writer_returns_escalated_payload_after_durable_reminder_gaps(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    from polyarb.control_plane import runtime_event_writer

    class Cursor:
        def __init__(self, latest_event: dict[str, object]) -> None:
            self.calls: list[str] = []
            self.latest_event = latest_event
            self.insert_parameters: tuple[object, ...] | None = None
            self.outbox_payloads: list[dict[str, object]] = []

        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, params=()):
            self.calls.append(sql)
            if "INSERT INTO m1_incident_events" in sql:
                self.insert_parameters = params
            if "INSERT INTO m1_alert_outbox" in sql:
                self.outbox_payloads.append(params[3])
        def fetchone(self):
            query_count = sum(not call.startswith("SET LOCAL") for call in self.calls)
            if query_count == 1:
                return None
            if query_count == 2:
                return {
                    "incident_key": "runtime-incident-a",
                    "state": "open",
                    "opened_at": "2030-01-01T00:00:00+00:00",
                }
            if query_count == 3:
                return self.latest_event
            if query_count == 4:
                return self.latest_event
            return None

    all_cursors = [
        Cursor({"kind": "detected", "occurred_at": "2030-01-01T00:00:00+00:00"}),
        Cursor({"kind": "escalated", "occurred_at": "2030-01-01T00:15:00+00:00"}),
        Cursor({"kind": "escalated", "occurred_at": "2030-01-01T00:15:00+00:00"}),
    ]
    cursors = list(all_cursors)

    class Connection:
        def __init__(self, cursor: Cursor) -> None:
            self._cursor = cursor
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def cursor(self, **_kwargs): return self._cursor

    def connect(_dsn: str, **_kwargs: object) -> Connection:
        return Connection(cursors.pop(0))

    monkeypatch.setattr(runtime_event_writer.psycopg, "connect", connect)
    monkeypatch.setattr(runtime_event_writer, "Jsonb", lambda value: value)
    app = Starlette(
        routes=[
            Route("/runtime-events", runtime_event_writer.append_runtime_event, methods=["POST"])
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        first = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "e" * 64},
            json=_runtime_transition_payload(occurred_at="2030-01-01T00:15:00+00:00"),
        )
        hourly = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "f" * 64},
            json=_runtime_transition_payload(occurred_at="2030-01-01T01:14:00+00:00"),
        )
        later_hourly = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "1" * 64},
            json=_runtime_transition_payload(occurred_at="2030-01-01T01:15:00+00:00"),
        )

    assert first.status_code == 201
    assert hourly.status_code == 201
    assert later_hourly.status_code == 201
    assert first.json()["transition_payload"]["transition"] == "escalated"
    assert hourly.json() == {"status": "noop"}
    assert later_hourly.json()["transition_payload"]["transition"] == "escalated"
    assert [len(cursor.outbox_payloads) for cursor in all_cursors] == [2, 0, 2]
    assert "ESCALATED" in render_runtime_incident_message(all_cursors[0].outbox_payloads[0])
    assert "ESCALATED" in render_runtime_incident_message(all_cursors[2].outbox_payloads[0])


def test_runtime_transition_writer_records_recovered_once_and_suppresses_replay(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    from polyarb.control_plane import runtime_event_writer

    class Cursor:
        def __init__(self, incident: dict[str, object]) -> None:
            self.calls: list[str] = []
            self.incident = incident
            self.outbox_payloads: list[dict[str, object]] = []

        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, params=()):
            self.calls.append(sql)
            if "INSERT INTO m1_alert_outbox" in sql:
                self.outbox_payloads.append(params[3])
        def fetchone(self):
            query_count = sum(not call.startswith("SET LOCAL") for call in self.calls)
            if query_count == 1:
                return None
            if query_count == 2:
                return self.incident
            return None

    all_cursors = [
        Cursor(
            {
                "incident_key": "runtime-incident-a",
                "state": "open",
                "opened_at": "2030-01-01T00:00:00+00:00",
            }
        ),
        Cursor(
            {
                "incident_key": "runtime-incident-a",
                "state": "resolved",
                "opened_at": "2030-01-01T00:00:00+00:00",
            }
        ),
    ]
    cursors = list(all_cursors)

    class Connection:
        def __init__(self, cursor: Cursor) -> None:
            self._cursor = cursor
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def cursor(self, **_kwargs): return self._cursor

    def connect(_dsn: str, **_kwargs: object) -> Connection:
        return Connection(cursors.pop(0))

    monkeypatch.setattr(runtime_event_writer.psycopg, "connect", connect)
    monkeypatch.setattr(runtime_event_writer, "Jsonb", lambda value: value)
    app = Starlette(
        routes=[
            Route("/runtime-events", runtime_event_writer.append_runtime_event, methods=["POST"])
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        recovered = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "2" * 64},
            json=_runtime_transition_payload(
                transition="recovered",
                occurred_at="2030-01-01T00:02:00+00:00",
            ),
        )
        replay = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "3" * 64},
            json=_runtime_transition_payload(
                transition="recovered",
                occurred_at="2030-01-01T00:03:00+00:00",
            ),
        )

    assert recovered.status_code == 201
    assert replay.status_code == 201
    assert recovered.json()["transition_payload"]["transition"] == "recovered"
    assert replay.json() == {"status": "noop"}
    assert [len(cursor.outbox_payloads) for cursor in all_cursors] == [2, 0]
    assert "RECOVERED" in render_runtime_incident_message(all_cursors[0].outbox_payloads[0])


def test_runtime_transition_writer_rejects_stale_recovered_before_latest_detected(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    from polyarb.control_plane import runtime_event_writer

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.outbox_payloads: list[dict[str, object]] = []

        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, params=()):
            self.calls.append(sql)
            if "INSERT INTO m1_alert_outbox" in sql:
                self.outbox_payloads.append(params[3])
        def fetchone(self):
            query_count = sum(not call.startswith("SET LOCAL") for call in self.calls)
            if query_count == 1:
                return None
            if query_count == 2:
                return {
                    "incident_key": "runtime-incident-a",
                    "state": "open",
                    "opened_at": "2030-01-01T00:00:00+00:00",
                }
            if query_count == 3:
                return {"kind": "detected", "occurred_at": "2030-01-01T00:10:00+00:00"}
            return None

    cursor = Cursor()

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def cursor(self, **_kwargs): return cursor

    monkeypatch.setattr(
        runtime_event_writer.psycopg,
        "connect",
        lambda *_args, **_kwargs: Connection(),
    )
    monkeypatch.setattr(runtime_event_writer, "Jsonb", lambda value: value)
    app = Starlette(
        routes=[
            Route("/runtime-events", runtime_event_writer.append_runtime_event, methods=["POST"])
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        response = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "4" * 64},
            json=_runtime_transition_payload(
                transition="recovered",
                occurred_at="2030-01-01T00:05:00+00:00",
            ),
        )

    assert response.status_code == 201
    assert response.json() == {"status": "noop"}
    assert not any("UPDATE m1_incidents SET state='resolved'" in call for call in cursor.calls)
    assert cursor.outbox_payloads == []


def test_runtime_transition_writer_rejects_stale_detected_before_latest_recovered(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    from polyarb.control_plane import runtime_event_writer

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.outbox_payloads: list[dict[str, object]] = []

        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, params=()):
            self.calls.append(sql)
            if "INSERT INTO m1_alert_outbox" in sql:
                self.outbox_payloads.append(params[3])
        def fetchone(self):
            query_count = sum(not call.startswith("SET LOCAL") for call in self.calls)
            if query_count == 1:
                return None
            if query_count == 2:
                return {
                    "incident_key": "runtime-incident-a",
                    "state": "resolved",
                    "opened_at": "2030-01-01T00:00:00+00:00",
                }
            if query_count == 3:
                return {"kind": "recovered", "occurred_at": "2030-01-01T00:10:00+00:00"}
            return None

    cursor = Cursor()

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def cursor(self, **_kwargs): return cursor

    monkeypatch.setattr(
        runtime_event_writer.psycopg,
        "connect",
        lambda *_args, **_kwargs: Connection(),
    )
    monkeypatch.setattr(runtime_event_writer, "Jsonb", lambda value: value)
    app = Starlette(
        routes=[
            Route("/runtime-events", runtime_event_writer.append_runtime_event, methods=["POST"])
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        response = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "5" * 64},
            json=_runtime_transition_payload(occurred_at="2030-01-01T00:05:00+00:00"),
        )

    assert response.status_code == 201
    assert response.json() == {"status": "noop"}
    assert not any("SET state='open'" in call for call in cursor.calls)
    assert cursor.outbox_payloads == []


def test_runtime_transition_writer_uses_recovery_started_as_ordering_not_reminder_cursor(
    monkeypatch,
) -> None:
    monkeypatch.setenv("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "test-token")
    from polyarb.control_plane import runtime_event_writer

    class Cursor:
        def __init__(self, *, occurred_at: str) -> None:
            self.calls: list[str] = []
            self.occurred_at = occurred_at
            self.outbox_payloads: list[dict[str, object]] = []

        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, params=()):
            self.calls.append(sql)
            if "INSERT INTO m1_alert_outbox" in sql:
                self.outbox_payloads.append(params[3])
        def fetchone(self):
            query_count = sum(not call.startswith("SET LOCAL") for call in self.calls)
            if query_count == 1:
                return None
            if query_count == 2:
                return {
                    "incident_key": "runtime-incident-a",
                    "state": "open",
                    "opened_at": "2030-01-01T00:00:00+00:00",
                }
            if query_count == 3:
                return {"kind": "recovery-started", "occurred_at": "2030-01-01T00:10:00+00:00"}
            if query_count == 4:
                return {"kind": "detected", "occurred_at": "2030-01-01T00:00:00+00:00"}
            return None

    all_cursors = [
        Cursor(occurred_at="2030-01-01T00:11:00+00:00"),
        Cursor(occurred_at="2030-01-01T00:15:00+00:00"),
    ]
    cursors = list(all_cursors)

    class Connection:
        def __init__(self, cursor: Cursor) -> None:
            self._cursor = cursor
        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def cursor(self, **_kwargs): return self._cursor

    def connect(_dsn: str, **_kwargs: object) -> Connection:
        return Connection(cursors.pop(0))

    monkeypatch.setattr(runtime_event_writer.psycopg, "connect", connect)
    monkeypatch.setattr(runtime_event_writer, "Jsonb", lambda value: value)
    app = Starlette(
        routes=[
            Route("/runtime-events", runtime_event_writer.append_runtime_event, methods=["POST"])
        ]
    )
    app.state.dsn = "postgresql://not-used"
    with TestClient(app) as client:
        early = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "6" * 64},
            json=_runtime_transition_payload(occurred_at=all_cursors[0].occurred_at),
        )
        first_reminder = client.post(
            "/runtime-events",
            headers={"Authorization": "Bearer test-token", "Idempotency-Key": "7" * 64},
            json=_runtime_transition_payload(occurred_at=all_cursors[1].occurred_at),
        )

    assert early.status_code == 201
    assert first_reminder.status_code == 201
    assert early.json() == {"status": "noop"}
    assert first_reminder.json()["transition_payload"]["transition"] == "escalated"
    assert [len(cursor.outbox_payloads) for cursor in all_cursors] == [0, 2]
