"""Read-only collection of one atomic neg-risk CLOB quote run."""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Protocol

from loguru import logger

from polyarb.routing.neg_risk_quote_store import (
    QUOTE_RUN_LEASE_MS,
    NegRiskQuoteStore,
    PersistedQuote,
    QuoteRun,
    QuoteRunLeaseLostError,
    UniverseLeg,
)
from polyarb.routing.neg_risk_quote_store import (
    QuoteUniverseUnavailableError as QuoteUniverseUnavailableError,
)


class QuoteCollectionIntegrityError(RuntimeError):
    """The CLOB response cannot safely be associated with the requested universe."""

    def __init__(self) -> None:
        super().__init__("clob-response-integrity-failed")


class QuoteFetchTimeoutError(TimeoutError):
    """The bounded CLOB fetch stage exceeded its configured deadline."""

    def __init__(self) -> None:
        super().__init__("clob-fetch-timeout")


class BooksReader(Protocol):
    """The read-only slice of ``ClobReaderClient`` required by collection."""

    async def get_books(
        self,
        token_ids: list[str],
        *,
        projection: str = "full",
    ) -> Sequence[Any]: ...


@dataclass(frozen=True)
class QuoteCollectionResult:
    run_id: int
    status: str
    universe_snapshot_id: int
    requested_token_count: int
    successful_response_count: int
    quote_taken_at_ms: int
    elapsed_ms: int
    universe_hash: str = ""
    attempt_id: int = 0
    universe_ms: int = 0
    admission_ms: int = 0
    fetch_ms: int = 0
    transform_ms: int = 0
    persist_ms: int = 0
    structure_receipt_digest: str = ""

    @classmethod
    def from_run(
        cls,
        run: QuoteRun,
        *,
        elapsed_ms: int,
        attempt_id: int = 0,
        universe_ms: int = 0,
        admission_ms: int = 0,
        fetch_ms: int = 0,
        transform_ms: int = 0,
        persist_ms: int = 0,
        structure_receipt_digest: str = "",
    ) -> QuoteCollectionResult:
        return cls(
            run_id=run.run_id,
            status=run.status,
            universe_snapshot_id=run.universe_snapshot_id,
            requested_token_count=run.requested_token_count,
            successful_response_count=run.successful_response_count,
            quote_taken_at_ms=run.quoted_at_ms,
            elapsed_ms=elapsed_ms,
            universe_hash=run.universe_hash,
            attempt_id=attempt_id,
            universe_ms=universe_ms,
            admission_ms=admission_ms,
            fetch_ms=fetch_ms,
            transform_ms=transform_ms,
            persist_ms=persist_ms,
            structure_receipt_digest=structure_receipt_digest,
        )


_MISSING = object()
_QUOTE_RUN_LEASE_RENEWAL_S = QUOTE_RUN_LEASE_MS / 3_000
QUOTE_FETCH_TIMEOUT_EXIT_CODE = 75


async def collect_neg_risk_quotes(
    *,
    quote_store: NegRiskQuoteStore,
    reader: BooksReader,
    now_ms: Callable[[], int] | None = None,
    lease_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    attempt_id: int = 0,
    fetch_timeout_s: float = 100.0,
) -> QuoteCollectionResult:
    """Collect every latest-universe YES quote into one complete-or-failed run.

    The snapshot universe is checked before acquiring a run lock or making any
    CLOB request. Once the durable run begins, no exception can promote a
    partial set: the run is best-effort marked failed and the original error is
    re-raised for the caller to classify. While the CLOB request is pending,
    this collector renews its own lease; losing that lease cancels the request
    so the collector can never finish alongside a replacement run.
    """
    clock = now_ms or quote_store.current_time_ms
    if isinstance(fetch_timeout_s, bool) or fetch_timeout_s <= 0:
        raise ValueError("fetch_timeout_s must be positive")
    stage_started = time.perf_counter()
    logger.info("neg-risk quote projection phase started phase=source-projection")
    universe = await asyncio.to_thread(quote_store.latest_verified_universe)
    universe_ms = int((time.perf_counter() - stage_started) * 1000)
    if attempt_id:
        await asyncio.to_thread(
            quote_store.checkpoint_collection_attempt,
            attempt_id,
            phase="admission",
            target_count=len(universe.legs),
            structure_receipt_digest=universe.structure_receipt_digest,
            phase_timings={"universe_ms": universe_ms},
        )
    logger.info(
        "neg-risk quote projection phase complete "
        f"phase=source-projection elapsed_ms={universe_ms} "
        f"target_count={len(universe.legs)} "
        f"structure_mode={universe.structure_mode} "
        f"source_revision={universe.structure_revision}"
    )
    legs = universe.legs
    quote_taken_at_ms = clock()
    stage_started = time.perf_counter()
    run_id = await asyncio.to_thread(
        quote_store.begin_verified_run,
        universe,
        quoted_at_ms=quote_taken_at_ms,
    )
    begin_ms = int((time.perf_counter() - stage_started) * 1000)
    if attempt_id:
        await asyncio.to_thread(
            quote_store.checkpoint_collection_attempt,
            attempt_id,
            phase="fetch",
            quote_run_id=run_id,
            phase_timings={"universe_ms": universe_ms, "admission_ms": begin_ms},
        )
    logger.info(
        "neg-risk quote admission phase complete "
        f"phase=run-admission run_id={run_id} elapsed_ms={begin_ms}"
    )
    failure_reason = "collector-error"
    try:
        if not legs:
            completed = await asyncio.to_thread(
                quote_store.complete_run,
                run_id,
                completed_at_ms=clock(),
                successful_response_count=0,
            )
            timings = {"universe_ms": universe_ms, "admission_ms": begin_ms}
            if attempt_id:
                await asyncio.to_thread(
                    quote_store.checkpoint_collection_attempt,
                    attempt_id,
                    phase="certify",
                    phase_timings=timings,
                )
            return QuoteCollectionResult.from_run(
                completed,
                elapsed_ms=0,
                attempt_id=attempt_id,
                universe_ms=universe_ms,
                admission_ms=begin_ms,
                structure_receipt_digest=universe.structure_receipt_digest,
            )
        token_ids = list({leg.yes_token_id: None for leg in legs})
        try:
            stage_started = time.perf_counter()
            async with asyncio.timeout(fetch_timeout_s):
                books = await _get_books_with_lease(
                    quote_store=quote_store,
                    reader=reader,
                    token_ids=token_ids,
                    run_id=run_id,
                    clock=clock,
                    lease_sleep=lease_sleep,
                )
            fetch_ms = int((time.perf_counter() - stage_started) * 1000)
            if attempt_id:
                await asyncio.to_thread(
                    quote_store.checkpoint_collection_attempt,
                    attempt_id,
                    phase="transform",
                    phase_timings={
                        "universe_ms": universe_ms,
                        "admission_ms": begin_ms,
                        "fetch_ms": fetch_ms,
                    },
                )
        except QuoteRunLeaseLostError:
            raise
        except TimeoutError as error:
            failure_reason = "clob-fetch-timeout"
            raise QuoteFetchTimeoutError() from error
        except Exception:
            failure_reason = "clob-fetch-failed"
            raise
        stage_started = time.perf_counter()
        indexed_count, terminal_quotes = await asyncio.to_thread(
            _build_terminal_quotes,
            books,
            token_ids,
            legs,
        )
        transform_ms = int((time.perf_counter() - stage_started) * 1000)
        if attempt_id:
            await asyncio.to_thread(
                quote_store.checkpoint_collection_attempt,
                attempt_id,
                phase="persist",
                phase_timings={
                    "universe_ms": universe_ms,
                    "admission_ms": begin_ms,
                    "fetch_ms": fetch_ms,
                    "transform_ms": transform_ms,
                },
            )
        stage_started = time.perf_counter()
        await asyncio.to_thread(
            quote_store.record_terminal_quotes,
            run_id,
            terminal_quotes,
        )
        completed_at_ms = clock()
        completed = await asyncio.to_thread(
            quote_store.complete_run,
            run_id,
            completed_at_ms=completed_at_ms,
            successful_response_count=indexed_count,
            publish_current_generation=True,
        )
        persist_ms = int((time.perf_counter() - stage_started) * 1000)
    except QuoteRunLeaseLostError:
        failure_reason = "collector-lease-lost"
        await asyncio.to_thread(
            _best_effort_fail,
            quote_store,
            run_id,
            failure_reason,
        )
        raise
    except QuoteCollectionIntegrityError:
        failure_reason = "clob-response-integrity-failed"
        await asyncio.to_thread(
            _best_effort_fail,
            quote_store,
            run_id,
            failure_reason,
        )
        raise
    except Exception:
        await asyncio.to_thread(
            _best_effort_fail,
            quote_store,
            run_id,
            failure_reason,
        )
        raise
    logger.info(
        "neg-risk quote collection stages "
        f"run_id={run_id} "
        f"universe_ms={universe_ms} "
        f"begin_ms={begin_ms} "
        f"fetch_ms={fetch_ms} "
        f"transform_ms={transform_ms} "
        f"persist_ms={persist_ms}"
    )
    timings = {
        "universe_ms": universe_ms,
        "admission_ms": begin_ms,
        "fetch_ms": fetch_ms,
        "transform_ms": transform_ms,
        "persist_ms": persist_ms,
    }
    if attempt_id:
        await asyncio.to_thread(
            quote_store.checkpoint_collection_attempt,
            attempt_id,
            phase="certify",
            phase_timings=timings,
        )
    return QuoteCollectionResult.from_run(
        completed,
        elapsed_ms=completed_at_ms - quote_taken_at_ms,
        attempt_id=attempt_id,
        universe_ms=universe_ms,
        admission_ms=begin_ms,
        fetch_ms=fetch_ms,
        transform_ms=transform_ms,
        persist_ms=persist_ms,
        structure_receipt_digest=universe.structure_receipt_digest,
    )


def _wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


async def _get_books_with_lease(
    *,
    quote_store: NegRiskQuoteStore,
    reader: BooksReader,
    token_ids: list[str],
    run_id: int,
    clock: Callable[[], int],
    lease_sleep: Callable[[float], Awaitable[None]],
) -> Sequence[Any]:
    """Await CLOB while proving that this task still owns its durable lease."""
    reader_task = asyncio.create_task(reader.get_books(token_ids, projection="top"))
    renewal_task = asyncio.create_task(
        _renew_lease_until_cancelled(
            quote_store=quote_store,
            run_id=run_id,
            clock=clock,
            lease_sleep=lease_sleep,
        )
    )
    try:
        done, _ = await asyncio.wait(
            (reader_task, renewal_task), return_when=asyncio.FIRST_COMPLETED
        )
        if renewal_task in done:
            try:
                renewal_task.result()
            except Exception as error:
                await _cancel_task(reader_task)
                raise QuoteRunLeaseLostError() from error
            await _cancel_task(reader_task)
            raise QuoteRunLeaseLostError()

        books = reader_task.result()
        try:
            await asyncio.to_thread(
                quote_store.renew_run_lease,
                run_id,
            )
        except Exception as error:
            raise QuoteRunLeaseLostError() from error
        return books
    finally:
        await _cancel_task(renewal_task, suppress_errors=True)
        await _cancel_task(reader_task, suppress_errors=True)


async def _renew_lease_until_cancelled(
    *,
    quote_store: NegRiskQuoteStore,
    run_id: int,
    clock: Callable[[], int],
    lease_sleep: Callable[[float], Awaitable[None]],
) -> None:
    while True:
        await lease_sleep(_QUOTE_RUN_LEASE_RENEWAL_S)
        await asyncio.to_thread(
            quote_store.renew_run_lease,
            run_id,
        )


async def _cancel_task(task: asyncio.Task[object], *, suppress_errors: bool = False) -> None:
    if not task.done():
        task.cancel()
    if suppress_errors:
        with suppress(asyncio.CancelledError, Exception):
            await task
        return
    with suppress(asyncio.CancelledError):
        await task


def _best_effort_fail(quote_store: NegRiskQuoteStore, run_id: int, failure_reason: str) -> None:
    try:
        quote_store.fail_run(run_id, failure_reason=failure_reason)
    except Exception:
        # The original collection error remains the observable exception.
        pass


def _deduplicated_legs(legs: tuple[UniverseLeg, ...]) -> tuple[UniverseLeg, ...]:
    by_token: dict[str, UniverseLeg] = {}
    for leg in legs:
        by_token.setdefault(leg.yes_token_id, leg)
    return tuple(by_token.values())


def _index_books_by_token(books: Sequence[Any], token_ids: list[str]) -> dict[str, Any]:
    if not isinstance(books, (list, tuple)):
        raise QuoteCollectionIntegrityError()
    requested = set(token_ids)
    indexed: dict[str, Any] = {}
    for book in books:
        asset_id = _field(book, "asset_id")
        asks = _field(book, "asks")
        if not isinstance(asset_id, str) or not asset_id or asset_id not in requested:
            raise QuoteCollectionIntegrityError()
        if asset_id in indexed or not isinstance(asks, (list, tuple)):
            raise QuoteCollectionIntegrityError()
        indexed[asset_id] = book
    return indexed


def _build_terminal_quotes(
    books: Sequence[Any],
    token_ids: list[str],
    legs: tuple[UniverseLeg, ...],
) -> tuple[int, tuple[PersistedQuote, ...]]:
    """Build the bounded top-of-book terminal set outside the event loop."""
    indexed_books = _index_books_by_token(books, token_ids)
    terminal_quotes = tuple(
        _terminal_quote_for_leg(leg, indexed_books.get(leg.yes_token_id))
        for leg in _deduplicated_legs(legs)
    )
    return len(indexed_books), terminal_quotes


def _terminal_quote_for_leg(leg: UniverseLeg, book: Any | None) -> PersistedQuote:
    if book is None:
        return _non_executable(leg, "missing-book")
    asks = _field(book, "asks")
    assert isinstance(asks, (list, tuple))
    if not asks:
        return _non_executable(leg, "missing-ask")

    best: tuple[float, float] | None = None
    saw_invalid_price = False
    for ask in asks:
        price = _finite_number(_field(ask, "price"), lower=0, upper=1)
        if price is None:
            saw_invalid_price = True
            continue
        size = _finite_number(_field(ask, "size"), lower=0)
        if size is None:
            continue
        if best is None or price < best[0]:
            best = (price, size)
    if best is not None:
        return PersistedQuote(
            leg.neg_risk_market_id,
            leg.market_id,
            leg.condition_id,
            leg.slug,
            leg.yes_token_id,
            "executable",
            best[0],
            best[1],
            event_id=leg.event_id,
            membership_hash=leg.membership_hash,
        )
    return _non_executable(leg, "invalid-ask-price" if saw_invalid_price else "invalid-ask-size")


def _non_executable(leg: UniverseLeg, terminal_state: str) -> PersistedQuote:
    return PersistedQuote(
        leg.neg_risk_market_id,
        leg.market_id,
        leg.condition_id,
        leg.slug,
        leg.yes_token_id,
        terminal_state,
        None,
        None,
        event_id=leg.event_id,
        membership_hash=leg.membership_hash,
    )


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, _MISSING)
    return getattr(value, name, _MISSING)


def _finite_number(value: Any, *, lower: float, upper: float | None = None) -> float | None:
    if isinstance(value, bool) or value is _MISSING:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= lower:
        return None
    if upper is not None and number > upper:
        return None
    return number
