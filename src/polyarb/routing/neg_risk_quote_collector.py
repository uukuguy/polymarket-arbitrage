"""Read-only collection of one atomic neg-risk CLOB quote run."""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from polyarb.routing.neg_risk_quote_store import (
    NegRiskQuoteStore,
    PersistedQuote,
    UniverseLeg,
)


class QuoteUniverseUnavailableError(RuntimeError):
    """The latest snapshot cannot provide an eligible quote universe."""

    def __init__(self) -> None:
        super().__init__("quote-universe-unavailable")


class QuoteCollectionIntegrityError(RuntimeError):
    """The CLOB response cannot safely be associated with the requested universe."""

    def __init__(self) -> None:
        super().__init__("clob-response-integrity-failed")


class BooksReader(Protocol):
    """The read-only slice of ``ClobReaderClient`` required by collection."""

    async def get_books(self, token_ids: list[str]) -> Sequence[Any]: ...


@dataclass(frozen=True)
class QuoteCollectionResult:
    run_id: int
    status: str
    universe_snapshot_id: int
    requested_token_count: int
    successful_response_count: int
    quote_taken_at_ms: int
    elapsed_ms: int


_MISSING = object()


async def collect_neg_risk_quotes(
    *,
    quote_store: NegRiskQuoteStore,
    reader: BooksReader,
    now_ms: Callable[[], int] | None = None,
) -> QuoteCollectionResult:
    """Collect every latest-universe YES quote into one complete-or-failed run.

    The snapshot universe is checked before acquiring a run lock or making any
    CLOB request. Once the durable run begins, no exception can promote a
    partial set: the run is best-effort marked failed and the original error is
    re-raised for the caller to classify.
    """
    clock = now_ms or _wall_clock_ms
    universe = quote_store.latest_universe()
    if universe is None or not universe[2]:
        raise QuoteUniverseUnavailableError()
    universe_snapshot_id, universe_taken_at_ms, legs = universe
    token_ids = list({leg.yes_token_id: None for leg in legs})
    quote_taken_at_ms = clock()
    run_id = quote_store.begin_run(
        universe_snapshot_id=universe_snapshot_id,
        universe_taken_at_ms=universe_taken_at_ms,
        legs=legs,
        quoted_at_ms=quote_taken_at_ms,
    )
    failure_reason = "collector-error"
    try:
        try:
            books = await reader.get_books(token_ids)
        except Exception:
            failure_reason = "clob-fetch-failed"
            raise
        indexed_books = _index_books_by_token(books, token_ids)
        terminal_quotes = tuple(
            _terminal_quote_for_leg(leg, indexed_books.get(leg.yes_token_id))
            for leg in _deduplicated_legs(legs)
        )
        quote_store.record_terminal_quotes(run_id, terminal_quotes)
        completed_at_ms = clock()
        completed = quote_store.complete_run(run_id, completed_at_ms=completed_at_ms)
    except QuoteCollectionIntegrityError:
        failure_reason = "clob-response-integrity-failed"
        _best_effort_fail(quote_store, run_id, failure_reason)
        raise
    except Exception:
        _best_effort_fail(quote_store, run_id, failure_reason)
        raise
    return QuoteCollectionResult(
        run_id=completed.run_id,
        status=completed.status,
        universe_snapshot_id=completed.universe_snapshot_id,
        requested_token_count=completed.requested_token_count,
        successful_response_count=completed.successful_response_count,
        quote_taken_at_ms=completed.quoted_at_ms,
        elapsed_ms=completed_at_ms - quote_taken_at_ms,
    )


def _wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000


def _best_effort_fail(
    quote_store: NegRiskQuoteStore, run_id: int, failure_reason: str
) -> None:
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


def _index_books_by_token(
    books: Sequence[Any], token_ids: list[str]
) -> dict[str, Any]:
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
        )
    return _non_executable(
        leg, "invalid-ask-price" if saw_invalid_price else "invalid-ask-size"
    )


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
    )


def _field(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, _MISSING)
    return getattr(value, name, _MISSING)


def _finite_number(
    value: Any, *, lower: float, upper: float | None = None
) -> float | None:
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
