"""Bounded, restart-safe producer for the online Structure staging window."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol

from polyarb.clients.gamma_client import EventPage, MarketPage
from polyarb.storage.sqlite_store import SQLiteStore


class StructureGamma(Protocol):
    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage: ...

    async def fetch_active_market_page(self, cursor: str | None, limit: int) -> MarketPage: ...


@dataclass(frozen=True)
class StructureSyncBatch:
    window_id: str
    stage: str
    completed: bool


class StructureSyncWorker:
    """Advance exactly one Gamma page; publication is intentionally elsewhere."""

    def __init__(self, *, gamma: StructureGamma, store: SQLiteStore, page_limit: int = 100) -> None:
        if not 1 <= page_limit <= 100:
            raise ValueError("structure-sync-page-limit-must-be-within-1..100")
        self._gamma = gamma
        self._store = store
        self._page_limit = page_limit

    async def run_batch(self) -> StructureSyncBatch:
        window = await asyncio.to_thread(
            self._store.begin_or_resume_structure_sync,
            started_at_ms=0,
        )
        status = str(window["status"])
        if status == "open":
            page = await self._gamma.fetch_active_event_page(
                window["event_cursor"], self._page_limit
            )
            if page.requested_cursor != window["event_cursor"]:
                raise ValueError("structure-event-page-cursor-mismatch")
            await asyncio.to_thread(
                self._store.commit_structure_event_page,
                window_id=window["id"], requested_cursor=page.requested_cursor,
                next_cursor=page.next_cursor, completed=page.completed,
                events=list(page.events), finished_at_ms=page.finished_at_ms,
            )
            return StructureSyncBatch(str(window["id"]), "events", page.completed)
        if status == "events_complete":
            page = await self._gamma.fetch_active_market_page(
                window["market_cursor"], self._page_limit
            )
            if page.requested_cursor != window["market_cursor"]:
                raise ValueError("structure-market-page-cursor-mismatch")
            await asyncio.to_thread(
                self._store.commit_structure_market_page,
                window_id=window["id"], requested_cursor=page.requested_cursor,
                next_cursor=page.next_cursor, completed=page.completed,
                markets=list(page.markets), finished_at_ms=page.finished_at_ms,
            )
            return StructureSyncBatch(str(window["id"]), "markets", page.completed)
        return StructureSyncBatch(str(window["id"]), "complete", True)
