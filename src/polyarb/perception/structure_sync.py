"""Bounded, restart-safe producer for the online Structure staging window."""

from __future__ import annotations

import asyncio
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import Protocol

from polyarb.clients.gamma_client import EventPage, GammaClient, MarketPage, PaginationResult
from polyarb.config import Settings
from polyarb.storage.sqlite_store import SQLiteStore


class StructureGamma(Protocol):
    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage: ...

    async def fetch_active_market_page(self, cursor: str | None, limit: int) -> MarketPage: ...


class ReconciliationGamma(Protocol):
    async def fetch_market_states(self, market_ids: list[str]): ...

    async def fetch_market_parent_states(self, market_groups: dict[str, str]): ...


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
            started_at_ms=int(time.time() * 1_000),
        )
        status = str(window["status"])
        if status == "open":
            started = time.monotonic()
            _emit_stage("gamma-events", "start", 0)
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
            _emit_stage(
                "gamma-events",
                "complete",
                max(0, int((time.monotonic() - started) * 1_000)),
            )
            return StructureSyncBatch(str(window["id"]), "events", page.completed)
        if status == "events_complete":
            started = time.monotonic()
            _emit_stage("gamma-markets", "start", 0)
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
            _emit_stage(
                "gamma-markets",
                "complete",
                max(0, int((time.monotonic() - started) * 1_000)),
            )
            return StructureSyncBatch(str(window["id"]), "markets", page.completed)
        return StructureSyncBatch(str(window["id"]), "complete", True)


def _emit_stage(stage: str, state: str, elapsed_ms: int) -> None:
    print(
        f"snapshot-stage stage={stage} state={state} elapsed_ms={elapsed_ms}",
        file=sys.stderr,
        flush=True,
    )


class StagedGammaSource:
    """Present one complete durable window through the existing Gamma interface."""

    def __init__(
        self,
        events: list[dict],
        markets: list[dict],
        *,
        point_client: ReconciliationGamma | None = None,
    ) -> None:
        self._events = deque(events)
        self._markets = deque(markets)
        self._event_count = len(self._events)
        self._market_count = len(self._markets)
        events.clear()
        markets.clear()
        self._point_client = point_client

    async def __aenter__(self) -> StagedGammaSource:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def iter_active_events(self, coverage):
        while self._events:
            yield self._events.popleft()
        coverage.result = PaginationResult(self._event_count, 1, True, None)

    async def iter_active_markets(self, coverage):
        while self._markets:
            yield self._markets.popleft()
        coverage.result = PaginationResult(self._market_count, 1, True, None)

    async def fetch_market_states(self, market_ids: list[str]):
        if self._point_client is None:
            raise RuntimeError(f"staged-structure-member-missing:{len(market_ids)}")
        return await self._point_client.fetch_market_states(market_ids)

    async def fetch_market_parent_states(self, market_groups: dict[str, str]):
        if self._point_client is None:
            raise RuntimeError(f"staged-structure-parent-missing:{len(market_groups)}")
        return await self._point_client.fetch_market_parent_states(market_groups)


class SQLiteStagedGammaSource:
    """Stream one completed Structure window directly from durable staging."""

    def __init__(
        self,
        store: SQLiteStore,
        window_id: str,
        *,
        point_client: ReconciliationGamma | None = None,
    ) -> None:
        self._store = store
        self._window_id = window_id
        self._event_count, self._market_count = (
            store.get_complete_structure_sync_counts(window_id)
        )
        self._point_client = point_client

    async def __aenter__(self) -> SQLiteStagedGammaSource:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def iter_active_events(self, coverage):
        for event in self._store.iter_complete_structure_events(self._window_id):
            yield event
        coverage.result = PaginationResult(self._event_count, 1, True, None)

    async def iter_active_markets(self, coverage):
        for market in self._store.iter_complete_structure_markets(self._window_id):
            yield market
        coverage.result = PaginationResult(self._market_count, 1, True, None)

    async def fetch_market_states(self, market_ids: list[str]):
        if self._point_client is None:
            raise RuntimeError(f"staged-structure-member-missing:{len(market_ids)}")
        return await self._point_client.fetch_market_states(market_ids)

    async def fetch_market_parent_states(self, market_groups: dict[str, str]):
        if self._point_client is None:
            raise RuntimeError(f"staged-structure-parent-missing:{len(market_groups)}")
        return await self._point_client.fetch_market_parent_states(market_groups)


async def finalize_structure_window(
    settings: Settings,
    window_id: str,
    *,
    now_ms: int,
    point_client: ReconciliationGamma | None = None,
):
    """Validate and atomically publish a completed window as Structure truth."""
    from polyarb.snapshot.orchestrator import run_snapshot

    store = SQLiteStore(settings.db_path)
    store.init_structure_sync_schema()
    result = await run_snapshot(
        settings,
        mode="full",
        product="structure",
        now_ms=now_ms,
        gamma_client=SQLiteStagedGammaSource(
            store,
            window_id,
            point_client=point_client,
        ),
        schema_ready=True,
    )
    if result.is_valid:
        await asyncio.to_thread(
            store.mark_structure_sync_published,
            window_id=window_id,
            snapshot_id=result.snapshot_id,
            published_at_ms=now_ms,
        )
    return result


async def run_structure_sync_until_published(settings: Settings):
    """Continuously checkpoint bounded pages until one certified revision publishes."""
    store = SQLiteStore(settings.db_path)
    store.init_structure_sync_schema()
    latest = store.get_latest_structure_sync()
    async with GammaClient(settings) as gamma:
        if latest is not None and latest["status"] == "complete":
            return await finalize_structure_window(
                settings,
                str(latest["id"]),
                now_ms=int(time.time() * 1_000),
                point_client=gamma,
            )
        worker = StructureSyncWorker(gamma=gamma, store=store)
        while True:
            batch = await worker.run_batch()
            if batch.stage == "markets" and batch.completed:
                return await finalize_structure_window(
                    settings,
                    batch.window_id,
                    now_ms=int(time.time() * 1_000),
                    point_client=gamma,
                )
