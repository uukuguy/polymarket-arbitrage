"""Durable, single-owner reconciliation for the L2 candidate chain.

Postgres NOTIFY is a doorbell, not a queue.  The durable cursor is the ledger:
notifications and a periodic timer merely wake one serialized pump, which reads
the latest snapshot and advances the cursor only after awaited business success.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

import asyncpg
from loguru import logger


@dataclass
class ReconciliationState:
    """Live facts shared by listener, reconciliation, and later health checks."""

    is_connected: bool = False
    reconnect_count: int = 0
    last_notification_s: float | None = None
    last_reconciliation_success_s: float | None = None
    latest_snapshot_id: int = 0
    committed_cursor: int = 0
    cursor_lag: int = 0
    cursor_lag_since_s: float | None = None
    last_error: str | None = None

    @property
    def is_listening(self) -> bool:
        """Backward-compatible health name; value is actual connection truth."""
        return self.is_connected


class CursorStore(Protocol):
    async def read_position(self) -> tuple[int, int]: ...

    async def commit(self, snapshot_id: int) -> None: ...


class AsyncpgCursorStore:
    """Short-lived asyncpg access to one named consumer's durable cursor."""

    def __init__(self, *, dsn: str, consumer: str = "l2-candidate-refresh") -> None:
        self._dsn = dsn
        self._consumer = consumer

    async def read_position(self) -> tuple[int, int]:
        conn = await asyncpg.connect(dsn=self._dsn)
        try:
            cursor_row = await conn.fetchrow(
                "SELECT last_snapshot_id FROM l2_event_cursor WHERE consumer=$1",
                self._consumer,
            )
            latest_row = await conn.fetchrow(
                "SELECT COALESCE(MAX(id), 0) AS latest_snapshot_id FROM snapshots"
            )
            cursor = int(cursor_row["last_snapshot_id"] or 0) if cursor_row else 0
            latest = int(latest_row["latest_snapshot_id"] or 0) if latest_row else 0
            return cursor, latest
        finally:
            await conn.close()

    async def commit(self, snapshot_id: int) -> None:
        if (
            isinstance(snapshot_id, bool)
            or not isinstance(snapshot_id, int)
            or snapshot_id < 0
        ):
            raise ValueError("snapshot_id must be a non-negative integer")
        conn = await asyncpg.connect(dsn=self._dsn)
        try:
            await conn.execute(
                "INSERT INTO l2_event_cursor (consumer, last_snapshot_id, updated_at) "
                "VALUES ($1, $2, now()) ON CONFLICT (consumer) DO UPDATE SET "
                "last_snapshot_id=EXCLUDED.last_snapshot_id, updated_at=now()",
                self._consumer,
                snapshot_id,
            )
        finally:
            await conn.close()


RefreshCallback = Callable[[dict], Awaitable[bool]]


class ReconciliationPump:
    """Coalesce wake hints and reconcile durable state one pass at a time."""

    def __init__(
        self,
        *,
        store: CursorStore,
        refresh: RefreshCallback,
        state: ReconciliationState,
        poll_seconds: float = 60.0,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        self.store = store
        self.refresh = refresh
        self.state = state
        self.poll_seconds = poll_seconds
        self.wake_event = asyncio.Event()

    def notify(self, payload: dict | None = None) -> None:
        """Record valid notification metadata and coalesce the wake hint."""
        snapshot_id = payload.get("snapshot_id") if isinstance(payload, dict) else None
        if (
            isinstance(snapshot_id, int)
            and not isinstance(snapshot_id, bool)
            and snapshot_id >= 0
        ):
            self.state.last_notification_s = time.time()
        self.wake_event.set()

    async def reconcile_once(self) -> bool:
        """Run one durable pass; retain the old cursor on every failure."""
        try:
            cursor, latest = await self.store.read_position()
            self.state.committed_cursor = cursor
            self.state.latest_snapshot_id = latest
            self.state.cursor_lag = max(0, latest - cursor)
            if self.state.cursor_lag > 0:
                if self.state.cursor_lag_since_s is None:
                    self.state.cursor_lag_since_s = time.time()
            else:
                self.state.cursor_lag_since_s = None

            if latest <= cursor:
                succeeded = await self.refresh(
                    {"snapshot_id": latest, "ts_s": time.time(), "_maintenance": True}
                )
                if not succeeded:
                    self.state.last_error = "candidate maintenance returned false"
                    return False
                self.state.last_reconciliation_success_s = time.time()
                self.state.last_error = None
                return True

            succeeded = await self.refresh(
                {"snapshot_id": latest, "ts_s": time.time(), "_reconciliation": True}
            )
            if not succeeded:
                self.state.last_error = "candidate refresh returned false"
                return False

            await self.store.commit(latest)
            self.state.committed_cursor = latest
            self.state.cursor_lag = 0
            self.state.cursor_lag_since_s = None
            self.state.last_reconciliation_success_s = time.time()
            self.state.last_error = None
            return True
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - fail-soft, truth retained in state
            self.state.last_error = f"{type(exc).__name__}: {exc}"
            logger.warning(f"event reconciliation failed: {type(exc).__name__}")
            return False

    async def run(self, stop_event: asyncio.Event) -> None:
        """Wake immediately, on NOTIFY, or on timer; never overlap passes."""
        self.wake_event.set()
        while not stop_event.is_set():
            if not self.wake_event.is_set():
                wake_task = asyncio.create_task(self.wake_event.wait())
                stop_task = asyncio.create_task(stop_event.wait())
                try:
                    done, _ = await asyncio.wait(
                        {wake_task, stop_task},
                        timeout=self.poll_seconds,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    if stop_task in done and stop_event.is_set():
                        return
                finally:
                    for task in (wake_task, stop_task):
                        if not task.done():
                            task.cancel()
                    await asyncio.gather(wake_task, stop_task, return_exceptions=True)

            self.wake_event.clear()
            await self.reconcile_once()
