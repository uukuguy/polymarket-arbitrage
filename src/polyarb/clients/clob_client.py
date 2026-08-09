"""Async wrapper around py-clob-client v0.34.6 (sync SDK).

Pattern: ``asyncio.to_thread`` + manual chunking at ``settings.clob_batch_size``
(default 500, the CLOB max-per-call). Full mode returns SDK responses verbatim;
snapshot callers use the bounded top-of-book projection.

Why two methods (``get_books`` + ``get_prices_buy_sell``):
- The CLOB ``order_book.bids[]`` is the canonical liquidity source (sizes).
- The CLOB ``get_prices`` endpoint is a separate ground-truth price source.
- Plan 3 Layer 4 validator cross-references them to detect ghost-books
  (Polymarket issue #180 — books without trades / fake liquidity).

Why no retry:
- py-clob-client has no async retry hook, and ``asyncio.to_thread`` blocks the
  worker. Wrapping in tenacity would either block the loop or require a thread
  pool dance that isn't worth the complexity. Let exceptions propagate;
  the orchestrator (Plan 4) categorizes them. If Plan 5 reveals frequent
  CLOB failures we can add a manual retry loop here as a follow-up.

Why no wallet code:
- L0 read-only endpoints (get_order_books, get_prices) require neither
  ``chain_id`` nor ``key`` nor ``creds``. See RESEARCH.md Pattern 2.

Empirical shapes (from fixtures/clob_sample.json, recorded T1 against live API):
- ``OrderBookSummary``: dataclass-like with ``market`` (=conditionId),
  ``asset_id`` (=token_id, the canonical id field), ``bids``, ``asks``,
  ``timestamp``, ``hash``, etc.
- ``get_prices`` returns ``{token_id: {"BUY"|"SELL": "<price-as-string>"}}``.
  Merging two side calls yields ``{"buy": {tid: {"BUY": "0.46"}}, "sell": ...}``.
"""

from __future__ import annotations

import asyncio
import math
from concurrent.futures import Executor
from typing import Any, Literal

import httpx
from aiolimiter import AsyncLimiter
from loguru import logger
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams
from py_clob_client.exceptions import PolyApiException
from py_clob_client.http_helpers import helpers as clob_http_helpers

from polyarb.config import Settings


def _chunked(seq: list[str], size: int) -> list[list[str]]:
    """Split ``seq`` into chunks of ``size`` (last may be shorter). Empty → []."""
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def _field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _compact_level(level: Any) -> dict[str, Any]:
    return {
        "price": _field(level, "price"),
        "size": _field(level, "size"),
    }


def _compact_best_level(levels: Any, *, ask: bool) -> list[dict[str, Any]]:
    """Select the executable top level without trusting CLOB list order."""
    if not isinstance(levels, (list, tuple)) or not levels:
        return []

    ranked: list[tuple[float, Any]] = []
    for level in levels:
        try:
            price = float(_field(level, "price"))
        except (TypeError, ValueError):
            continue
        if math.isfinite(price):
            ranked.append((price, level))

    if not ranked:
        # Preserve one malformed level so downstream validation records the
        # external-input defect instead of silently treating the book as empty.
        return [_compact_level(levels[0])]
    _, best = (
        min(ranked, key=lambda item: item[0]) if ask else max(ranked, key=lambda item: item[0])
    )
    return [_compact_level(best)]


def _compact_book_top(book: Any) -> dict[str, Any]:
    """Drop full depth while retaining exactly what snapshot validation reads."""
    asks = _field(book, "asks")
    bids = _field(book, "bids")
    asset_id = _field(book, "asset_id") or _field(book, "market") or _field(book, "token_id")
    return {
        "asset_id": asset_id,
        # Polymarket production books are commonly worst-first (asks
        # descending, bids ascending). Rank prices before discarding depth.
        "asks": _compact_best_level(asks, ask=True),
        "bids": _compact_best_level(bids, ask=False),
    }


def _transport_error_kind(error: BaseException) -> str | None:
    """Return only the wrapped HTTP transport exception class, if present.

    ``py-clob-client`` converts every ``httpx.RequestError`` into the generic
    ``PolyApiException(error_msg="Request exception!")``.  Exception chaining
    remains available, but request text may contain token IDs or URLs, so the
    incident-safe diagnostic deliberately retains only the exception class.
    """
    current = error.__cause__ or error.__context__
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, httpx.RequestError):
            return type(current).__name__
        current = current.__cause__ or current.__context__
    return None


class ClobReaderClient:
    """Async wrapper over py-clob-client's sync read-only API.

    Construction is cheap: only stores settings, builds a sync ``ClobClient``
    (read-only L0 — no wallet, no chain_id), and an aiolimiter token bucket.
    No network I/O happens until a method is called.
    """

    _transport_configured = False

    def __init__(
        self,
        settings: Settings,
        *,
        executor: Executor | None = None,
    ) -> None:
        self._settings = settings
        self._configure_sdk_transport(settings.clob_batch_max_concurrency)
        # L0 read-only: only host needed. NO key/creds/chain_id (tested in T5).
        self._client = ClobClient(settings.clob_url)
        self._limiter = AsyncLimiter(settings.clob_batch_rate_per_10s, 10)
        self._batch_semaphore = asyncio.Semaphore(settings.clob_batch_max_concurrency)
        self._executor = executor

    @classmethod
    def _configure_sdk_transport(cls, max_connections: int) -> None:
        """Install one bounded HTTP/1.1 pool for the SDK in this process."""
        if cls._transport_configured:
            return
        previous = clob_http_helpers._http_client
        clob_http_helpers._http_client = httpx.Client(
            http2=False,
            limits=httpx.Limits(max_connections=max_connections),
        )
        previous.close()
        cls._transport_configured = True

    async def _run_sync(self, function: Any, *args: Any) -> Any:
        if self._executor is None:
            return await asyncio.to_thread(function, *args)
        return await asyncio.get_running_loop().run_in_executor(
            self._executor,
            function,
            *args,
        )

    async def get_books(
        self,
        token_ids: list[str],
        *,
        cache: Any | None = None,
        projection: Literal["full", "top"] = "full",
    ) -> list[Any]:
        """Fetch order books for ``token_ids`` (chunked at ``clob_batch_size``).

        ``projection="full"`` returns raw ``OrderBookSummary`` objects.
        ``projection="top"`` projects every fetched/cache chunk immediately
        to asset identity plus the best ask and bid. This keeps snapshot RSS bounded
        when the verified universe contains tens of thousands of tokens.

        Empty input returns ``[]`` without making any network call.

        When ``cache`` is provided (a ``ChunkCache`` instance), each chunk is
        persisted to disk after fetch and skipped on subsequent calls if a
        valid cached chunk is already present. cached chunks come back as
        plain dicts (the SDK's ``OrderBookSummary`` is rehydrated as
        ``__dict__`` form), which downstream ``_index_books_by_token``
        already accepts.
        """
        out: list[Any] = []
        if projection not in ("full", "top"):
            raise ValueError(f"unsupported book projection: {projection!r}")
        if not token_ids:
            return out

        chunks = _chunked(token_ids, self._settings.clob_batch_size)
        n_chunks = len(chunks)
        async def fetch_chunk(i: int, chunk: list[str]) -> list[Any]:
            if cache is not None and cache.has_books_chunk(i):
                cached = cache.load_books_chunk(i)
                if projection == "top":
                    result = [_compact_book_top(book) for book in cached]
                else:
                    result = cached
                logger.info(f"CLOB books chunk {i}/{n_chunks}: cached ({len(cached)} books)")
                return result
            params = [BookParams(token_id=t) for t in chunk]

            def fetch_sync() -> tuple[list[Any] | None, list[Any]]:
                try:
                    raw_books = self._client.get_order_books(params)
                except PolyApiException as error:
                    transport_kind = _transport_error_kind(error)
                    if transport_kind is not None:
                        logger.warning(
                            f"CLOB books chunk {i}/{n_chunks} failed: "
                            f"transport_kind={transport_kind}"
                        )
                    raise
                projected = (
                    [_compact_book_top(book) for book in raw_books]
                    if projection == "top"
                    else raw_books
                )
                return (raw_books if cache is not None else None), projected

            async with self._batch_semaphore:
                async with self._limiter:
                    raw_books, books = await self._run_sync(fetch_sync)
            if cache is not None and raw_books is not None:
                cache.save_books_chunk(i, raw_books)
            logger.info(f"CLOB books chunk {i}/{n_chunks}: fetched ({len(chunk)} tokens)")
            return books

        # gather preserves chunk order, so downstream identity validation sees
        # the same deterministic sequence as the previous serial collector.
        chunk_results = await asyncio.gather(
            *(fetch_chunk(i, chunk) for i, chunk in enumerate(chunks, start=1))
        )
        for books in chunk_results:
            out.extend(books)
        return out

    async def get_prices_buy_sell(
        self,
        token_ids: list[str],
        *,
        cache: Any | None = None,
    ) -> dict[str, dict[str, Any]]:
        """Fetch BUY and SELL prices for every token, returning a side-keyed dict.

        Shape (matches recorded fixture):
            {
                "buy":  {token_id: {"BUY":  "<price-as-string>"}},
                "sell": {token_id: {"SELL": "<price-as-string>"}},
            }

        Each side is a separate CLOB call (with its own batching), to keep
        BUY-side and SELL-side fetches independent and so a partial failure
        on one side surfaces clearly.

        Empty input returns ``{"buy": {}, "sell": {}}`` without any network call.

        ``cache`` (optional ``ChunkCache``) persists each chunk to disk and
        skips already-cached chunks on resume.
        """
        result: dict[str, dict[str, Any]] = {"buy": {}, "sell": {}}
        if not token_ids:
            return result

        chunks = _chunked(token_ids, self._settings.clob_batch_size)
        n_chunks = len(chunks)
        for side, side_label in (("BUY", "buy"), ("SELL", "sell")):
            acc: dict[str, Any] = {}
            for i, chunk in enumerate(chunks, start=1):
                if cache is not None and cache.has_prices_chunk(side, i):
                    cached = cache.load_prices_chunk(side, i)
                    acc.update(cached)
                    logger.info(
                        f"CLOB prices {side} chunk {i}/{n_chunks}: cached ({len(cached)} entries)"
                    )
                    continue
                params = [BookParams(token_id=t, side=side) for t in chunk]
                async with self._limiter:
                    page = await self._run_sync(self._client.get_prices, params)
                # CLOB get_prices returns dict-of-token-id; merge via update.
                # If a future SDK version returns a list, callers see TypeError
                # immediately rather than silent data loss.
                acc.update(page)
                if cache is not None:
                    cache.save_prices_chunk(side, i, page)
                logger.info(
                    f"CLOB prices {side} chunk {i}/{n_chunks}: fetched ({len(chunk)} tokens)"
                )
            result[side_label] = acc
        return result
