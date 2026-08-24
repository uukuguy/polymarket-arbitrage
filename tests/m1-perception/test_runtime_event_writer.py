"""Contract tests for the private watchdog event writer boundary."""

from starlette.applications import Starlette
from starlette.routing import Route
from starlette.testclient import TestClient

from polyarb.control_plane.runtime_event_writer import append_runtime_event, healthz


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

        def __enter__(self): return self
        def __exit__(self, *_args): return None
        def execute(self, sql, params=()):
            self.calls.append(sql)
            if not sql.startswith("SET LOCAL"):
                self.parameters.append(params)

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
    last_parameters = cursor.parameters[-1]
    assert isinstance(last_parameters, tuple)
    assert last_parameters[3] == {
        "failures": ["control-api:timeout"],
        "source": "cloudflare-watchdog-supervisor",
    }
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
