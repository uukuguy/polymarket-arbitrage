"""L1 → L2 cross-process NOTIFY publisher (D-05).

`publish_snapshot_complete` opens a short-lived asyncpg connection, fires a
single `pg_notify('snapshot_complete', payload_json)`, and closes the
connection. Fail-soft per D-12 invariant: any exception is caught, logged
at warning level, surfaced as a Sentry breadcrumb (category='event-bus'),
and the function returns False instead of raising.

Phase 02.2 backlog preemptive (RESEARCH Open Q 9): the SUCCESS path also
emits an info-level breadcrumb so the absence of any event-bus breadcrumbs
in a Sentry event signals "listener offline" rather than "no events".

Payload shape:
    {"snapshot_id": <int>, "taken_at_ms": <int>}

Size discipline: realistic payloads are ~80 bytes, well under Postgres'
8000-byte NOTIFY hard limit. The test suite enforces this invariant.
"""
from __future__ import annotations

import json
from typing import Any

import asyncpg
import sentry_sdk
from loguru import logger


async def publish_snapshot_complete(
    settings: Any,
    *,
    snapshot_id: int,
    taken_at_ms: int,
) -> bool:
    """Fire a pg_notify('snapshot_complete', json_payload). Fail-soft.

    Args:
        settings: object exposing ``supabase_db_dsn`` (SecretStr) — typically
                  a polyarb Settings instance.
        snapshot_id: snapshot row id (int).
        taken_at_ms: snapshot wall-clock ms (int).

    Returns:
        True on success, False on any failure (NEVER raises).
    """
    payload = json.dumps({"snapshot_id": snapshot_id, "taken_at_ms": taken_at_ms})
    try:
        dsn = settings.supabase_db_dsn.get_secret_value()
        conn = await asyncpg.connect(dsn=dsn)
        try:
            await conn.execute(
                "SELECT pg_notify('snapshot_complete', $1)",
                payload,
            )
        finally:
            try:
                await conn.close()
            except Exception:  # noqa: BLE001
                # closing is best-effort; never let it mask the success
                pass
        # Phase 02.2 backlog preemptive — success path breadcrumb so listener
        # offline state is detectable from absence of breadcrumbs.
        sentry_sdk.add_breadcrumb(
            category="event-bus",
            level="info",
            message=f"published snapshot_complete snapshot_id={snapshot_id}",
            data={"snapshot_id": snapshot_id, "taken_at_ms": taken_at_ms},
        )
        logger.info(
            f"event_bus published snapshot_complete snapshot_id={snapshot_id} "
            f"taken_at_ms={taken_at_ms}"
        )
        return True
    except Exception as e:  # noqa: BLE001 — fail-soft per D-12
        logger.warning(f"event bus publish failed (fail-soft): {e!r}")
        sentry_sdk.add_breadcrumb(
            category="event-bus",
            level="warning",
            message=f"publish snapshot_complete failed: {snapshot_id}",
            data={"error": str(e)[:200]},
        )
        return False
