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
from typing import Any, Literal

from aiolimiter import AsyncLimiter
from loguru import logger
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams

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


class ClobReaderClient:
    """Async wrapper over py-clob-client's sync read-only API.

    Construction is cheap: only stores settings, builds a sync ``ClobClient``
    (read-only L0 — no wallet, no chain_id), and an aiolimiter token bucket.
    No network I/O happens until a method is called.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # L0 read-only: only host needed. NO key/creds/chain_id (tested in T5).
        self._client = ClobClient(settings.clob_url)
        self._limiter = AsyncLimiter(settings.clob_batch_rate_per_10s, 10)

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
        for i, chunk in enumerate(chunks, start=1):
            if cache is not None and cache.has_books_chunk(i):
                cached = cache.load_books_chunk(i)
                if projection == "top":
                    out.extend(_compact_book_top(book) for book in cached)
                else:
                    out.extend(cached)
                logger.info(f"CLOB books chunk {i}/{n_chunks}: cached ({len(cached)} books)")
                continue
            params = [BookParams(token_id=t) for t in chunk]

            def fetch_chunk() -> tuple[list[Any] | None, list[Any]]:
                raw_books = self._client.get_order_books(params)
                projected = (
                    [_compact_book_top(book) for book in raw_books]
                    if projection == "top"
                    else raw_books
                )
                return (raw_books if cache is not None else None), projected

            async with self._limiter:
                raw_books, books = await asyncio.to_thread(fetch_chunk)
            if cache is not None and raw_books is not None:
                cache.save_books_chunk(i, raw_books)
            out.extend(books)
            logger.info(f"CLOB books chunk {i}/{n_chunks}: fetched ({len(chunk)} tokens)")
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
                    page = await asyncio.to_thread(self._client.get_prices, params)
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
