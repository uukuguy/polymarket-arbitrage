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
    assert invalid.status_code == 400
