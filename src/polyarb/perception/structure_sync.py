"""Bounded, restart-safe producer for the online Structure staging window."""

from __future__ import annotations

import asyncio
import sys
import time
from collections import deque
from dataclasses import dataclass
from typing import Protocol

from loguru import logger

from polyarb.clients.gamma_client import (
    EventPage,
    GammaClient,
    MarketPage,
    PaginationCursorRejectedError,
    PaginationResult,
)
from polyarb.config import Settings
from polyarb.perception.structure_contract import (
    STRUCTURE_PUBLICATION_MAX_ROWS,
    STRUCTURE_PUBLICATION_MIN_CHUNK_REMAINING_S,
)
from polyarb.storage.sqlite_store import SQLiteStore, StructureMembershipInvalidError

_monotonic = time.monotonic

# One Gamma page must leave enough of the cooperative child slice for its
# durable SQLite checkpoint and orderly process shutdown.  Without this bound,
# Gamma's own retry policy can outlive the slice and force the parent to kill
# the child, losing the actionable cause of the stall.
STRUCTURE_REMOTE_PAGE_MAX_ELAPSED_S = 35.0
# Gamma retries use a shared client-wide request timeout. Keep each attempt at
# ten seconds so the production default (three attempts plus 1s/2s backoff)
# remains below the 35-second page envelope and can checkpoint or terminalize
# before the 75-second subprocess kill.
STRUCTURE_REMOTE_PAGE_REQUEST_TIMEOUT_S = 10.0
# The online child must leave enough time to report a busy writer and retry the
# same cursor. Offline/import callers retain SQLiteStore's longer default.
STRUCTURE_PAGE_COMMIT_WRITER_TIMEOUT_S = 5.0


class StructurePageDeadlineExceeded(ValueError):
    """A single Gamma page exhausted the Structure cooperative request budget."""

    def __init__(self) -> None:
        super().__init__("structure-page-deadline")


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


@dataclass(frozen=True)
class StructureSyncCheckpoint:
    """Durable progress returned when one cooperative producer slice ends."""

    window_id: str
    stage: str
    pages_processed: int


class StructureSyncWorker:
    """Advance exactly one Gamma page; publication is intentionally elsewhere."""

    def __init__(self, *, gamma: StructureGamma, store: SQLiteStore, page_limit: int = 100) -> None:
        if not 1 <= page_limit <= 100:
            raise ValueError("structure-sync-page-limit-must-be-within-1..100")
        self._gamma = gamma
        self._store = store
        self._page_limit = page_limit

    async def run_batch(self, *, page_timeout_s: float | None = None) -> StructureSyncBatch:
        if page_timeout_s is not None and page_timeout_s <= 0:
            raise ValueError("structure-page-timeout-must-be-positive")
        window = await asyncio.to_thread(
            self._store.begin_or_resume_structure_sync,
            started_at_ms=int(time.time() * 1_000),
        )
        status = str(window["status"])
        if status == "open":
            started = time.monotonic()
            _emit_stage("gamma-events", "start", 0)
            _emit_page_boundary("gamma-events", "fetch", "start", 0)
            try:
                if page_timeout_s is None:
                    page = await self._gamma.fetch_active_event_page(
                        window["event_cursor"], self._page_limit
                    )
                else:
                    async with asyncio.timeout(page_timeout_s):
                        page = await self._gamma.fetch_active_event_page(
                            window["event_cursor"], self._page_limit
                        )
            except TimeoutError as error:
                raise StructurePageDeadlineExceeded() from error
            _emit_page_boundary(
                "gamma-events", "fetch", "complete", _elapsed_ms(started)
            )
            if page.requested_cursor != window["event_cursor"]:
                raise ValueError("structure-event-page-cursor-mismatch")
            _emit_page_boundary("gamma-events", "commit", "start", _elapsed_ms(started))
            await asyncio.to_thread(
                self._store.commit_structure_event_page,
                window_id=window["id"], requested_cursor=page.requested_cursor,
                next_cursor=page.next_cursor, completed=page.completed,
                events=list(page.events), finished_at_ms=page.finished_at_ms,
                writer_timeout_s=STRUCTURE_PAGE_COMMIT_WRITER_TIMEOUT_S,
            )
            _emit_page_boundary("gamma-events", "commit", "complete", _elapsed_ms(started))
            _emit_stage(
                "gamma-events",
                "complete",
                _elapsed_ms(started),
            )
            return StructureSyncBatch(str(window["id"]), "events", page.completed)
        if status == "events_complete":
            started = time.monotonic()
            _emit_stage("gamma-markets", "start", 0)
            _emit_page_boundary("gamma-markets", "fetch", "start", 0)
            try:
                if page_timeout_s is None:
                    page = await self._gamma.fetch_active_market_page(
                        window["market_cursor"], self._page_limit
                    )
                else:
                    async with asyncio.timeout(page_timeout_s):
                        page = await self._gamma.fetch_active_market_page(
                            window["market_cursor"], self._page_limit
                        )
            except TimeoutError as error:
                raise StructurePageDeadlineExceeded() from error
            _emit_page_boundary(
                "gamma-markets", "fetch", "complete", _elapsed_ms(started)
            )
            if page.requested_cursor != window["market_cursor"]:
                raise ValueError("structure-market-page-cursor-mismatch")
            _emit_page_boundary("gamma-markets", "commit", "start", _elapsed_ms(started))
            await asyncio.to_thread(
                self._store.commit_structure_market_page,
                window_id=window["id"], requested_cursor=page.requested_cursor,
                next_cursor=page.next_cursor, completed=page.completed,
                markets=list(page.markets), finished_at_ms=page.finished_at_ms,
                writer_timeout_s=STRUCTURE_PAGE_COMMIT_WRITER_TIMEOUT_S,
            )
            _emit_page_boundary("gamma-markets", "commit", "complete", _elapsed_ms(started))
            _emit_stage(
                "gamma-markets",
                "complete",
                _elapsed_ms(started),
            )
            return StructureSyncBatch(str(window["id"]), "markets", page.completed)
        return StructureSyncBatch(str(window["id"]), "complete", True)


def _emit_stage(stage: str, state: str, elapsed_ms: int) -> None:
    print(
        f"snapshot-stage stage={stage} state={state} elapsed_ms={elapsed_ms}",
        file=sys.stderr,
        flush=True,
    )


def _elapsed_ms(started: float) -> int:
    return max(0, int((time.monotonic() - started) * 1_000))


def _emit_page_boundary(stage: str, operation: str, state: str, elapsed_ms: int) -> None:
    print(
        "structure-page-boundary "
        f"stage={stage} operation={operation} state={state} elapsed_ms={elapsed_ms}",
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


async def run_structure_sync_until_published(
    settings: Settings,
    *,
    max_pages: int | None = None,
    max_elapsed_s: float | None = None,
    max_publication_rows: int = 500,
    schema_ready: bool = False,
):
    """Checkpoint pages until publication or one cooperative slice ends."""
    if max_pages is not None and max_pages < 1:
        raise ValueError("structure-sync-max-pages-must-be-positive")
    if max_elapsed_s is not None and max_elapsed_s <= 0:
        raise ValueError("structure-sync-max-elapsed-must-be-positive")
    if not 1 <= max_publication_rows <= STRUCTURE_PUBLICATION_MAX_ROWS:
        raise ValueError("structure-publication-max-rows-must-be-positive")
    slice_started = _monotonic()
    store = SQLiteStore(settings.db_path)
    if not schema_ready:
        store.init_structure_sync_schema()
    latest = store.get_latest_structure_sync()
    bootstrap_chunks = 0
    bootstrap_complete = bool(
        latest is not None
        and latest["status"] == "complete"
        and store.structure_event_market_backfill_complete(str(latest["id"]))
    )
    if (
        latest is not None
        and latest["status"] == "complete"
        and not bootstrap_complete
        and (max_pages is not None or max_elapsed_s is not None)
    ):
        bootstrap_rows = 0
        bootstrap_completed = False
        while bootstrap_chunks < 100:
            if (
                max_elapsed_s is not None
                and _monotonic() - slice_started >= max_elapsed_s
            ):
                break
            migration = await asyncio.to_thread(
                store.advance_structure_event_market_backfill,
                window_id=str(latest["id"]),
                max_events=max_publication_rows,
                max_relationships=max_publication_rows,
                now_ms=int(time.time() * 1_000),
            )
            bootstrap_chunks += 1
            bootstrap_rows += max(
                int(migration["events_processed"]),
                int(migration["relationships_processed"]),
            )
            if migration["blocked"]:
                successor = await asyncio.to_thread(
                    store.rotate_blocked_structure_sync_window,
                    window_id=str(latest["id"]),
                    rotated_at_ms=int(time.time() * 1_000),
                )
                raise ValueError(
                    "structure-bootstrap-window-rotated:"
                    f"{latest['id']}:{successor['id']}:{migration['blocked_reason']}"
                )
            if migration["completed"]:
                bootstrap_completed = True
                bootstrap_complete = True
                break
            if (
                max_elapsed_s is not None
                and _monotonic() - slice_started >= max_elapsed_s
            ):
                break
        if not bootstrap_completed or bootstrap_chunks >= 100 or (
            max_elapsed_s is not None
            and _monotonic() - slice_started >= max_elapsed_s
        ):
            return StructureSyncCheckpoint(
                window_id=str(latest["id"]),
                stage="bootstrap",
                pages_processed=max(1, bootstrap_rows),
            )
        # Member/conflict authority must be admitted only after the terminal
        # relationship checkpoint is durable.  Yield this producer slice so
        # the scheduler's Quote-priority member child can seal before any
        # publication work begins.
        return StructureSyncCheckpoint(
            window_id=str(latest["id"]),
            stage="bootstrap",
            pages_processed=max(1, bootstrap_rows),
        )
    if (
        latest is not None
        and latest["status"] == "complete"
        and (max_pages is not None or max_elapsed_s is not None)
    ):
        from polyarb.perception.structure_publication import (
            StructurePublicationCheckpoint,
            run_structure_publication_slice,
        )

        remaining_s = (
            max_elapsed_s - (_monotonic() - slice_started)
            if max_elapsed_s is not None
            else 60.0
        )
        if (
            remaining_s < STRUCTURE_PUBLICATION_MIN_CHUNK_REMAINING_S
            and store.get_structure_publication_progress(str(latest["id"])) is None
        ):
            return StructureSyncCheckpoint(
                window_id=str(latest["id"]),
                stage="bootstrap",
                pages_processed=max(1, bootstrap_chunks),
            )
        try:
            return await asyncio.to_thread(
                run_structure_publication_slice,
                settings,
                str(latest["id"]),
                max_rows=max_publication_rows,
                max_elapsed_s=max(0.001, remaining_s),
                max_chunks=100 - bootstrap_chunks,
                store=store,
            )
        except StructureMembershipInvalidError:
            retired = await asyncio.to_thread(
                store.retire_membership_invalid_structure_publication,
                str(latest["id"]),
                now_ms=int(time.time() * 1_000),
            )
            return StructurePublicationCheckpoint(
                stage="superseded",
                component=None,
                rows_processed=0,
                cursor=None,
                publication_id=retired.publication_id,
            )
    gamma_settings = settings
    if max_elapsed_s is not None:
        gamma_settings = settings.model_copy(
            update={
                "http_timeout_s": min(
                    settings.http_timeout_s,
                    STRUCTURE_REMOTE_PAGE_REQUEST_TIMEOUT_S,
                )
            }
        )
    async with GammaClient(gamma_settings) as gamma:
        if latest is not None and latest["status"] == "complete":
            return await finalize_structure_window(
                settings,
                str(latest["id"]),
                now_ms=int(time.time() * 1_000),
                point_client=gamma,
            )
        worker = StructureSyncWorker(gamma=gamma, store=store)
        cursor_restarts = 0
        pages_processed = 0
        while True:
            # Preserve the existing cooperative "check after each durable
            # page" semantics while ensuring a page begun near the deadline
            # cannot run into the parent process-kill envelope.  The fixed
            # remote page cap is insufficient here: a 35-second request that
            # starts at second 40 of a 45-second slice is killed by the
            # 75-second parent before it can report its checkpoint.
            page_timeout_s = None
            if max_elapsed_s is not None:
                remaining_s = max_elapsed_s - (_monotonic() - slice_started)
                if remaining_s <= STRUCTURE_PAGE_COMMIT_WRITER_TIMEOUT_S:
                    active_window = store.get_latest_structure_sync()
                    if active_window is None:
                        raise RuntimeError("structure-sync-window-missing")
                    return StructureSyncCheckpoint(
                        window_id=str(active_window["id"]),
                        stage=(
                            "events"
                            if active_window["status"] == "open"
                            else "markets"
                        ),
                        pages_processed=max(1, pages_processed),
                    )
                page_timeout_s = min(
                    STRUCTURE_REMOTE_PAGE_MAX_ELAPSED_S,
                    remaining_s - STRUCTURE_PAGE_COMMIT_WRITER_TIMEOUT_S,
                )
            try:
                batch = await worker.run_batch(page_timeout_s=page_timeout_s)
            except PaginationCursorRejectedError as error:
                if cursor_restarts >= 1:
                    raise
                active = store.get_latest_structure_sync()
                if (
                    active is None
                    or active["status"] not in {"open", "events_complete"}
                ):
                    raise
                restarted_at_ms = int(time.time() * 1_000)
                successor = await asyncio.to_thread(
                    store.restart_structure_sync_window,
                    window_id=str(active["id"]),
                    restarted_at_ms=restarted_at_ms,
                    failure_reason=(
                        f"cursor-rejected:{error.source}:{error.status_code}"
                    ),
                )
                cursor_restarts += 1
                logger.warning(
                    "structure cursor rejected; rotated durable window "
                    f"source={error.source} status_code={error.status_code} "
                    f"failed_window_id={active['id']} "
                    f"successor_window_id={successor['id']}"
                )
                continue
            pages_processed += 1
            elapsed_budget_reached = (
                max_elapsed_s is not None
                and _monotonic() - slice_started >= max_elapsed_s
            )
            if (
                max_pages is not None and pages_processed >= max_pages
            ) or elapsed_budget_reached:
                return StructureSyncCheckpoint(
                    window_id=batch.window_id,
                    stage=batch.stage,
                    pages_processed=pages_processed,
                )
            if batch.stage == "markets" and batch.completed:
                remaining_s = (
                    max_elapsed_s - (_monotonic() - slice_started)
                    if max_elapsed_s is not None
                    else None
                )
                if remaining_s is None:
                    return await finalize_structure_window(
                        settings,
                        batch.window_id,
                        now_ms=int(time.time() * 1_000),
                        point_client=gamma,
                    )
                if remaining_s <= 0:
                    return StructureSyncCheckpoint(
                        window_id=batch.window_id,
                        stage=batch.stage,
                        pages_processed=pages_processed,
                    )
                return await run_structure_sync_until_published(
                    settings,
                    max_elapsed_s=remaining_s,
                    max_publication_rows=max_publication_rows,
                )
