"""L2 receive-side: LISTEN consumer + cursor catch-up (D-05).

`listen_snapshot_complete` runs an outer reconnect loop. It opens an
asyncpg connection, registers an `add_listener` callback that parses each
NOTIFY payload as JSON and forwards the parsed dict to `on_event`, then
issues `LISTEN snapshot_complete` and blocks on `stop_event.wait()`. If
the connection drops the outer loop logs a warning, sleeps 5s, and
reconnects. CancelledError propagates per F-04.

`catchup_from_cursor` is the startup procedure. It reads the consumer's
last seen snapshot_id from `l2_event_cursor` (created by Plan 06 Alembic
003) and returns every snapshot row from `snapshots` newer than that. If
the `l2_event_cursor` table does not yet exist (Plan 06 has not run), we
catch `UndefinedTableError` and return [] — Plan 05 lands first.

Callback dispatch contract: asyncpg invokes the registered callback in
the asyncpg event loop. If `on_event` is async, callers MUST wrap it in
`asyncio.create_task` themselves (see l2_main._dispatch_on_snapshot).
The bare sync wrapper here only parses + dispatches.
"""
from __future__ import annotations

import asyncio
import json
from typing import Callable

import asyncpg
import asyncpg.exceptions
from loguru import logger


def _make_callback(on_event: Callable[[dict], None]) -> Callable:
    """Build the sync callback asyncpg.add_listener will invoke.

    Signature: ``cb(conn, pid, channel, payload_str)``.
    Errors during JSON parse OR on_event dispatch are logged and swallowed
    — the listener MUST NOT crash on a single malformed message.
    """

    def _cb(conn, pid, channel, payload):  # noqa: ARG001
        try:
            data = json.loads(payload)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error(
                f"event listener: malformed payload, json parse failed: {e!r} "
                f"payload[:200]={str(payload)[:200]!r}"
            )
            return
        try:
            on_event(data)
        except Exception as e:  # noqa: BLE001
            logger.error(f"event listener: on_event raised: {e!r}")

    return _cb


async def listen_snapshot_complete(
    dsn: str,
    on_event: Callable[[dict], None],
    stop_event: asyncio.Event,
) -> None:
    """Outer reconnect loop: LISTEN snapshot_complete → dispatch to on_event.

    Terminates when stop_event is set OR when CancelledError is raised
    (F-04 contract — propagate, never swallow). Any other exception causes
    a 5s sleep + reconnect.
    """
    while not stop_event.is_set():
        try:
            conn = await asyncpg.connect(dsn=dsn)
            try:
                await conn.add_listener("snapshot_complete", _make_callback(on_event))
                await conn.execute("LISTEN snapshot_complete")
                logger.info("event listener connected to snapshot_complete channel")
                await stop_event.wait()
                return
            finally:
                try:
                    await conn.close()
                except Exception:  # noqa: BLE001
                    pass
        except asyncio.CancelledError:
            # F-04: MUST propagate.
            logger.info("event listener cancelled, propagating CancelledError")
            raise
        except Exception as e:  # noqa: BLE001
            if stop_event.is_set():
                return
            logger.warning(f"event listener reconnecting in 5s: {e!r}")
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise


async def catchup_from_cursor(
    dsn: str,
    consumer: str = "l2-candidate-refresh",
) -> list[dict]:
    """Replay snapshots missed while the listener was down.

    Reads ``l2_event_cursor.last_snapshot_id`` for the named consumer; if
    the row is missing, last_seen=0. Returns every row from ``snapshots``
    with id > last_seen, ordered by id.

    Defensive vs Plan 06: ``l2_event_cursor`` is created by Alembic 003
    (Plan 06). If we run before Plan 06 has shipped the migration, we get
    UndefinedTableError on the fetchrow — log INFO and return [].
    """
    try:
        conn = await asyncpg.connect(dsn=dsn)
    except Exception as e:  # noqa: BLE001
        logger.warning(f"catchup_from_cursor connect failed (fail-soft): {e!r}")
        return []
    try:
        try:
            cursor_row = await conn.fetchrow(
                "SELECT last_snapshot_id FROM l2_event_cursor WHERE consumer=$1",
                consumer,
            )
        except asyncpg.exceptions.UndefinedTableError:
            logger.info(
                "catchup_from_cursor: l2_event_cursor table missing "
                "(Plan 06 may not have shipped yet); skipping catchup"
            )
            return []
        last_seen = cursor_row["last_snapshot_id"] if cursor_row else 0
        try:
            missed = await conn.fetch(
                "SELECT id, taken_at_ms FROM snapshots WHERE id > $1 ORDER BY id",
                last_seen,
            )
        except asyncpg.exceptions.UndefinedTableError:
            logger.info(
                "catchup_from_cursor: snapshots table missing (fresh L1); "
                "skipping catchup"
            )
            return []
        return [dict(r) for r in missed]
    finally:
        try:
            await conn.close()
        except Exception:  # noqa: BLE001
            pass
