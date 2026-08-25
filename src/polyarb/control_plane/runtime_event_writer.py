"""Private, least-privilege append endpoint for watchdog dashboard events."""
# ruff: noqa: E501 -- SQL is intentionally kept as auditable single statements.

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import psycopg
import uvicorn
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from .alert_delivery import DEFAULT_RUNTIME_DASHBOARD_URL, runtime_incident_transition_payload

_FAILURE_CODE = re.compile(r"^[a-z0-9:/._-]{1,256}$")
_BOUNDED_TEXT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:/._ @#=+-]{0,255}$")
_BOUNDED_URL = re.compile(r"^https?://[^\s]{1,511}$")
_RUNTIME_TRANSITION_SCHEMA = "m1-runtime-incident-transition-v1"
_SECRET_WORDS = ("secret", "token", "password", "api_key", "apikey", "authorization")
_QUALIFICATION_IMPACTS = {
    "none",
    "unknown",
    "delayed",
    "invalidated",
    "recovering",
    "qualified",
    "breaking",
}
_RUNTIME_ACTIONS = {
    "none",
    "heartbeat-job",
    "cancel-job",
    "retry-job",
    "reclaim-job",
    "probe-circuit",
    "restart-worker-process",
    "restart-machine",
}


def _authorized(request: Request) -> bool:
    expected = os.environ.get("POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "")
    return bool(expected) and request.headers.get("authorization") == f"Bearer {expected}"


async def healthz(_request: Request) -> JSONResponse:
    return JSONResponse({"status": "ok"})


async def append_runtime_event(request: Request) -> JSONResponse:
    if not _authorized(request):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        payload = await request.json()
        transition = _runtime_transition_from_payload(payload)
        kind = transition["transition"]
        source = transition["source"]
        occurred_at = datetime.fromisoformat(str(transition["occurred_at"])).astimezone(UTC)
        key = request.headers["idempotency-key"]
        if not re.fullmatch(r"[0-9a-f]{64}", key):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        return JSONResponse({"error": "invalid-runtime-event"}, status_code=400)
    incident_dedupe_key = (
        "runtime-watchdog"
        if source == "independent-runtime-watchdog"
        else f"runtime-watchdog:{source}"
    )
    dsn = request.app.state.dsn
    with psycopg.connect(dsn, connect_timeout=5) as connection, connection.cursor(
        row_factory=dict_row
    ) as cursor:
        cursor.execute("SET LOCAL statement_timeout = '5000ms'")
        cursor.execute("SET LOCAL lock_timeout = '1000ms'")
        cursor.execute("SELECT incident_event_id FROM m1_incident_events WHERE idempotency_key=%s", (f"runtime:{key}",))
        existing = cursor.fetchone()
        if existing is not None:
            return JSONResponse({"status": "duplicate", "incident_event_id": existing["incident_event_id"]}, status_code=201)
        cursor.execute("SELECT incident_key, state, opened_at FROM m1_incidents WHERE dedupe_key=%s FOR UPDATE", (incident_dedupe_key,))
        row = cursor.fetchone()
        if kind == "detected":
            if row is None:
                generated_incident_key = str(uuid4())
                cursor.execute("""INSERT INTO m1_incidents (incident_key,dedupe_key,component,severity,state,summary,opened_at,updated_at)
                    VALUES (%s,%s,'runtime-watchdog','critical','open','Independent M1 runtime watchdog detected an unhealthy state',%s,%s)
                    ON CONFLICT (dedupe_key) DO NOTHING
                    RETURNING incident_key
                """, (generated_incident_key, incident_dedupe_key, occurred_at, occurred_at))
                incident = cursor.fetchone()
                if incident is not None:
                    incident_key = str(incident["incident_key"])
                    event_kind = "detected"
                else:
                    cursor.execute("SELECT incident_key, state, opened_at FROM m1_incidents WHERE dedupe_key=%s FOR UPDATE", (incident_dedupe_key,))
                    row = cursor.fetchone()
                    if row is None:
                        raise RuntimeError("runtime incident conflict returned no row")
                    incident_key = str(row["incident_key"])
                    event_kind = _runtime_existing_detected_event_kind(
                        cursor=cursor,
                        incident=row,
                        incident_key=incident_key,
                        occurred_at=occurred_at,
                    )
                    if event_kind is None:
                        return JSONResponse({"status": "noop"}, status_code=201)
            elif row.get("state") != "open":
                cursor.execute("""
                    UPDATE m1_incidents
                    SET state='open', severity='critical',
                        summary='Independent M1 runtime watchdog detected an unhealthy state',
                        opened_at=%s, updated_at=%s
                    WHERE dedupe_key=%s
                    RETURNING incident_key
                """, (occurred_at, occurred_at, incident_dedupe_key))
                incident = cursor.fetchone()
                if incident is None:
                    raise RuntimeError("runtime incident reopen returned no row")
                incident_key = str(incident["incident_key"])
                event_kind = "detected"
            else:
                incident_key = str(row["incident_key"])
                event_kind = _runtime_existing_detected_event_kind(
                    cursor=cursor,
                    incident=row,
                    incident_key=incident_key,
                    occurred_at=occurred_at,
                )
                if event_kind is None:
                    return JSONResponse({"status": "noop"}, status_code=201)
        else:
            if row is None:
                # A watchdog's first healthy observation has no preceding
                # incident.  It is a valid no-op, rather than a false writer
                # failure that would page Telegram.
                return JSONResponse({"status": "noop"}, status_code=201)
            if row.get("state") == "resolved":
                return JSONResponse({"status": "noop"}, status_code=201)
            incident_key = str(row["incident_key"])
            cursor.execute("UPDATE m1_incidents SET state='resolved', resolved_at=%s, updated_at=%s WHERE incident_key=%s", (occurred_at, occurred_at, incident_key))
            event_kind = "recovered"
        event_id = str(uuid4())
        transition_payload = _alert_transition_payload(
            transition,
            incident_key=incident_key,
            event_kind=event_kind,
            occurred_at=occurred_at,
        )
        cursor.execute("""INSERT INTO m1_incident_events (incident_event_id,incident_key,kind,detail,idempotency_key,occurred_at)
            VALUES (%s,%s,%s,%s,%s,%s)""", (event_id, incident_key, event_kind, Jsonb(_event_detail(transition)), f"runtime:{key}", occurred_at))
        for channel in ("dashboard", "telegram"):
            cursor.execute("""
                INSERT INTO m1_alert_outbox (
                    outbox_id, incident_event_id, channel, payload, state,
                    next_attempt_at, created_at
                ) VALUES (%s,%s,%s,%s,'pending',%s,%s)
                ON CONFLICT (incident_event_id, channel) DO NOTHING
            """, (str(uuid4()), event_id, channel, Jsonb(transition_payload), occurred_at, occurred_at))
    return JSONResponse({"status": "recorded", "incident_key": incident_key, "incident_event_id": event_id, "transition_payload": transition_payload}, status_code=201)


def _runtime_transition_from_payload(payload: object) -> dict[str, object]:
    if not isinstance(payload, Mapping):
        raise ValueError
    if payload.get("schema_version") != _RUNTIME_TRANSITION_SCHEMA:
        kind = payload["kind"]
        failures = payload["failures"]
        source = payload.get("source", "independent-runtime-watchdog")
        if kind not in {"detected", "recovered"} or not isinstance(failures, list) or not isinstance(source, str):
            raise ValueError
        if (
            len(failures) > 20
            or any(not isinstance(value, str) or not _FAILURE_CODE.fullmatch(value) for value in failures)
            or not _FAILURE_CODE.fullmatch(source)
        ):
            raise ValueError
        return {
            "schema_version": _RUNTIME_TRANSITION_SCHEMA,
            "legacy": True,
            "transition": kind,
            "failures": failures,
            "source": source,
            "component": "runtime-watchdog",
            "job_key": None,
            "stage": None,
            "reason": failures[0] if failures else "runtime-healthy",
            "action": "none" if kind == "recovered" else "restart-machine",
            "qualification_impact": "unknown",
            "dashboard_url": DEFAULT_RUNTIME_DASHBOARD_URL,
            "occurred_at": payload["occurred_at"],
        }
    transition = _payload_choice(payload, "transition", {"detected", "recovered"})
    source = _payload_text(payload, "source", max_len=128)
    component = _payload_text(payload, "component", max_len=128)
    incident_key = _payload_text(payload, "incident_key", max_len=256)
    if incident_key != f"runtime-watchdog:{source}":
        raise ValueError
    reason = _payload_text(payload, "reason", max_len=256)
    result = {
        "schema_version": _RUNTIME_TRANSITION_SCHEMA,
        "legacy": False,
        "transition": transition,
        "failures": [] if transition == "recovered" else [reason],
        "source": source,
        "component": component,
        "job_key": _payload_text(payload, "job_key", max_len=256, required=False),
        "stage": _payload_text(payload, "stage", max_len=128, required=False),
        "reason": reason,
        "action": _payload_choice(payload, "action", _RUNTIME_ACTIONS),
        "qualification_impact": _payload_choice(
            payload, "qualification_impact", _QUALIFICATION_IMPACTS
        ),
        "dashboard_url": _payload_text(
            payload, "dashboard_url", max_len=512, pattern=_BOUNDED_URL
        ),
        "occurred_at": _payload_text(payload, "occurred_at", max_len=64),
    }
    datetime.fromisoformat(str(result["occurred_at"])).astimezone(UTC)
    return result


def _payload_choice(payload: Mapping[str, object], field: str, allowed: set[str]) -> str:
    value = _payload_text(payload, field, max_len=128)
    if value not in allowed:
        raise ValueError
    return value


def _payload_text(
    payload: Mapping[str, object],
    field: str,
    *,
    max_len: int,
    required: bool = True,
    pattern: re.Pattern[str] = _BOUNDED_TEXT,
) -> str | None:
    value = payload.get(field)
    if value is None and not required:
        return None
    if not isinstance(value, str) or not value or len(value) > max_len:
        raise ValueError
    if not pattern.fullmatch(value):
        raise ValueError
    lower = value.lower()
    if any(word in lower for word in _SECRET_WORDS):
        raise ValueError
    return value


def _runtime_reminder_kind(
    *,
    incident: Mapping[str, object],
    latest_event: Mapping[str, object] | None,
    occurred_at: datetime,
) -> str | None:
    opened_at = _coerce_time(incident.get("opened_at"))
    if latest_event is None:
        return "detected"
    latest_kind = latest_event.get("kind")
    latest_at = _coerce_time(latest_event.get("occurred_at"))
    if latest_kind == "detected":
        return "escalated" if occurred_at - opened_at >= timedelta(minutes=15) else None
    if latest_kind == "escalated":
        return "escalated" if occurred_at - latest_at >= timedelta(hours=1) else None
    return None


def _runtime_existing_detected_event_kind(
    *,
    cursor: Any,
    incident: Mapping[str, object],
    incident_key: str,
    occurred_at: datetime,
) -> str | None:
    cursor.execute("""
        SELECT kind, occurred_at
        FROM m1_incident_events
        WHERE incident_key=%s AND kind IN ('detected','escalated')
        ORDER BY occurred_at DESC, incident_event_id DESC
        LIMIT 1
    """, (incident_key,))
    return _runtime_reminder_kind(
        incident=incident,
        latest_event=cursor.fetchone(),
        occurred_at=occurred_at,
    )


def _coerce_time(value: object) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str):
        return datetime.fromisoformat(value).astimezone(UTC)
    raise ValueError


def _event_detail(transition: Mapping[str, object]) -> dict[str, object]:
    if transition.get("legacy") is True:
        return {
            "failures": transition["failures"],
            "source": transition["source"],
        }
    return {
        "failures": transition["failures"],
        "source": transition["source"],
        "component": transition["component"],
        "job_key": transition["job_key"],
        "stage": transition["stage"],
        "reason": transition["reason"],
        "action": transition["action"],
        "qualification_impact": transition["qualification_impact"],
        "dashboard_url": transition["dashboard_url"],
    }


def _alert_transition_payload(
    transition: Mapping[str, object],
    *,
    incident_key: str,
    event_kind: str,
    occurred_at: datetime,
) -> dict[str, object]:
    return runtime_incident_transition_payload(
        transition=event_kind,
        incident_id=incident_key,
        incident_key=incident_key,
        component=str(transition["component"]),
        source=str(transition["source"]),
        job_key=None if transition["job_key"] is None else str(transition["job_key"]),
        stage=None if transition["stage"] is None else str(transition["stage"]),
        reason=str(transition["reason"]),
        action="none" if event_kind == "recovered" else str(transition["action"]),
        qualification_impact=str(transition["qualification_impact"]),
        dashboard_url=str(transition["dashboard_url"]),
        occurred_at=occurred_at,
    )


def main() -> int:
    dsn = os.environ.get("POLYARB_SUPABASE_DB_DSN", "")
    if not dsn:
        raise SystemExit("POLYARB_SUPABASE_DB_DSN is required")
    app = Starlette(routes=[Route("/healthz", healthz), Route("/runtime-events", append_runtime_event, methods=["POST"])])
    app.state.dsn = dsn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("POLYARB_HTTP_PORT", "8080")), log_config=None)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
