"""Private, least-privilege append endpoint for watchdog dashboard events."""
# ruff: noqa: E501 -- SQL is intentionally kept as auditable single statements.

from __future__ import annotations

import os
import re
from datetime import UTC, datetime
from uuid import uuid4

import psycopg
import uvicorn
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

_FAILURE_CODE = re.compile(r"^[a-z0-9:/._-]{1,256}$")


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
        kind = payload["kind"]
        failures = payload["failures"]
        source = payload.get("source", "independent-runtime-watchdog")
        occurred_at = datetime.fromisoformat(payload["occurred_at"]).astimezone(UTC)
        key = request.headers["idempotency-key"]
        if kind not in {"detected", "recovered"} or not isinstance(failures, list) or not isinstance(source, str):
            raise ValueError
        if (
            len(failures) > 20
            or any(not isinstance(value, str) or not _FAILURE_CODE.fullmatch(value) for value in failures)
            or not _FAILURE_CODE.fullmatch(source)
            or not re.fullmatch(r"[0-9a-f]{64}", key)
        ):
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
        cursor.execute("SELECT incident_key FROM m1_incidents WHERE dedupe_key=%s FOR UPDATE", (incident_dedupe_key,))
        row = cursor.fetchone()
        if kind == "detected":
            cursor.execute("""INSERT INTO m1_incidents (incident_key,dedupe_key,component,severity,state,summary,opened_at,updated_at)
                VALUES (%s,%s,'runtime-watchdog','critical','open','Independent M1 runtime watchdog detected an unhealthy state',%s,%s)
                ON CONFLICT (dedupe_key) DO UPDATE SET state='open', severity='critical', summary=EXCLUDED.summary, updated_at=EXCLUDED.updated_at
                RETURNING incident_key
            """, (str(uuid4()), incident_dedupe_key, occurred_at, occurred_at))
            incident = cursor.fetchone()
            if incident is None:
                raise RuntimeError("runtime incident insert returned no row")
            incident_key = str(incident["incident_key"])
        else:
            if row is None:
                # A watchdog's first healthy observation has no preceding
                # incident.  It is a valid no-op, rather than a false writer
                # failure that would page Telegram.
                return JSONResponse({"status": "noop"}, status_code=201)
            incident_key = str(row["incident_key"])
            cursor.execute("UPDATE m1_incidents SET state='resolved', resolved_at=%s, updated_at=%s WHERE incident_key=%s", (occurred_at, occurred_at, incident_key))
        event_id = str(uuid4())
        cursor.execute("""INSERT INTO m1_incident_events (incident_event_id,incident_key,kind,detail,idempotency_key,occurred_at)
            VALUES (%s,%s,%s,%s,%s,%s)""", (event_id, incident_key, kind, Jsonb({"failures": failures, "source": source}), f"runtime:{key}", occurred_at))
    return JSONResponse({"status": "recorded", "incident_key": incident_key, "incident_event_id": event_id}, status_code=201)


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
