"""L2 receive-side: LISTEN consumer + cursor catch-up (D-05).

`listen_snapshot_complete` runs an outer reconnect loop. It opens an
asyncpg connection, registers an `add_listener` callback that parses each
NOTIFY payload as JSON and forwards the parsed dict to `on_event`, then
issues `LISTEN snapshot_complete` and races process shutdown against the
asyncpg connection termination callback. Connection death therefore wakes
the outer loop and reconnects without a process restart. CancelledError
propagates per F-04.

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
from collections.abc import Callable

import asyncpg
import asyncpg.exceptions
from loguru import logger

_CONNECTION_CLOSE_TIMEOUT_S = 0.5


async def _close_connection(conn: object) -> None:
    """Bound transport cleanup without swallowing caller cancellation."""
    try:
        await asyncio.wait_for(conn.close(), timeout=_CONNECTION_CLOSE_TIMEOUT_S)
    except asyncio.CancelledError:
        raise
    except Exception:  # noqa: BLE001 - cleanup is fail-soft and bounded
        pass


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
            logger.error(f"event listener: malformed payload, json parse failed: {e!r}")
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
    state: object | None = None,
    *,
    initial_backoff_s: float = 1.0,
    max_backoff_s: float = 30.0,
) -> None:
    """Outer reconnect loop: LISTEN snapshot_complete → dispatch to on_event.

    Terminates when stop_event is set OR when CancelledError is raised
    (F-04 contract — propagate, never swallow). Any other exception causes
    a 5s sleep + reconnect.
    """
    backoff_s = initial_backoff_s
    while not stop_event.is_set():
        conn = None
        stop_task: asyncio.Task | None = None
        terminated: asyncio.Future | None = None
        try:
            conn = await asyncpg.connect(dsn=dsn)
            try:
                await conn.add_listener("snapshot_complete", _make_callback(on_event))
                await conn.execute("LISTEN snapshot_complete")
                terminated = asyncio.get_running_loop().create_future()

                def _on_termination(_conn: object) -> None:
                    if not terminated.done():
                        terminated.set_result(None)

                conn.add_termination_listener(_on_termination)
                if state is not None:
                    setattr(state, "is_connected", True)
                    setattr(state, "last_error", None)
                logger.info("event listener connected to snapshot_complete channel")
                backoff_s = initial_backoff_s
                stop_task = asyncio.create_task(stop_event.wait())
                done, _ = await asyncio.wait(
                    {stop_task, terminated}, return_when=asyncio.FIRST_COMPLETED
                )
                if stop_task in done and stop_event.is_set():
                    return
                if state is not None:
                    setattr(state, "is_connected", False)
                raise ConnectionError("LISTEN connection terminated")
            finally:
                if state is not None:
                    setattr(state, "is_connected", False)
                if stop_task is not None and not stop_task.done():
                    stop_task.cancel()
                    try:
                        await stop_task
                    except asyncio.CancelledError:
                        pass
                if terminated is not None and not terminated.done():
                    terminated.cancel()
                await _close_connection(conn)
        except asyncio.CancelledError:
            # F-04: MUST propagate.
            logger.info("event listener cancelled, propagating CancelledError")
            raise
        except Exception as e:  # noqa: BLE001
            if stop_event.is_set():
                return
            if state is not None:
                setattr(state, "is_connected", False)
                setattr(state, "reconnect_count", getattr(state, "reconnect_count", 0) + 1)
                setattr(state, "last_error", type(e).__name__)
            logger.warning(
                f"event listener reconnecting in {backoff_s:g}s: {type(e).__name__}"
            )
            try:
                await asyncio.sleep(backoff_s)
            except asyncio.CancelledError:
                raise
            backoff_s = min(max_backoff_s, max(initial_backoff_s, backoff_s * 2))


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
        await _close_connection(conn)
