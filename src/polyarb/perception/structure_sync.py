"""Bounded, restart-safe producer for the online Structure staging window."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol

from polyarb.clients.gamma_client import EventPage, MarketPage, PaginationResult
from polyarb.config import Settings
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


class StagedGammaSource:
    """Present one complete durable window through the existing Gamma interface."""

    def __init__(self, events: list[dict], markets: list[dict]) -> None:
        self._events = events
        self._markets = markets

    async def __aenter__(self) -> StagedGammaSource:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def iter_active_events(self, coverage):
        for event in self._events:
            yield event
        coverage.result = PaginationResult(len(self._events), 1, True, None)

    async def iter_active_markets(self, coverage):
        for market in self._markets:
            yield market
        coverage.result = PaginationResult(len(self._markets), 1, True, None)

    async def fetch_market_states(self, market_ids: list[str]):
        raise RuntimeError(f"staged-structure-member-missing:{len(market_ids)}")

    async def fetch_market_parent_states(self, market_groups: dict[str, str]):
        raise RuntimeError(f"staged-structure-parent-missing:{len(market_groups)}")


async def finalize_structure_window(
    settings: Settings,
    window_id: str,
    *,
    now_ms: int,
):
    """Validate and atomically publish a completed window as Structure truth."""
    from polyarb.snapshot.orchestrator import run_snapshot

    store = SQLiteStore(settings.db_path)
    store.init_schema()
    events, markets = await asyncio.to_thread(
        store.read_complete_structure_sync,
        window_id,
    )
    result = await run_snapshot(
        settings,
        mode="full",
        product="structure",
        now_ms=now_ms,
        gamma_client=StagedGammaSource(events, markets),
    )
    if result.is_valid:
        await asyncio.to_thread(
            store.mark_structure_sync_published,
            window_id=window_id,
            snapshot_id=result.snapshot_id,
            published_at_ms=now_ms,
        )
    return result
