"""Async wrapper around py-clob-client v0.34.6 (sync SDK).

Pattern: ``asyncio.to_thread`` + manual chunking at ``settings.clob_batch_size``
(default 500, the CLOB max-per-call). Returns RAW responses verbatim from the
SDK; normalization is owned by Plan 4.

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
from typing import Any

from aiolimiter import AsyncLimiter
from loguru import logger
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams

from polyarb.config import Settings


def _chunked(seq: list[str], size: int) -> list[list[str]]:
    """Split ``seq`` into chunks of ``size`` (last may be shorter). Empty → []."""
    return [seq[i : i + size] for i in range(0, len(seq), size)]


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
    ) -> list[Any]:
        """Fetch order books for ``token_ids`` (chunked at ``clob_batch_size``).

        Returns a list of ``OrderBookSummary`` objects (dataclass-like; key
        attrs: ``market``, ``asset_id``, ``bids``, ``asks``, ``timestamp``).
        Plan 4 normalizes; this layer returns raw SDK output.

        Empty input returns ``[]`` without making any network call.

        When ``cache`` is provided (a ``ChunkCache`` instance), each chunk is
        persisted to disk after fetch and skipped on subsequent calls if a
        valid cached chunk is already present. cached chunks come back as
        plain dicts (the SDK's ``OrderBookSummary`` is rehydrated as
        ``__dict__`` form), which downstream ``_index_books_by_token``
        already accepts.
        """
        out: list[Any] = []
        if not token_ids:
            return out

        chunks = _chunked(token_ids, self._settings.clob_batch_size)
        n_chunks = len(chunks)
        for i, chunk in enumerate(chunks, start=1):
            if cache is not None and cache.has_books_chunk(i):
                cached = cache.load_books_chunk(i)
                out.extend(cached)
                logger.info(f"CLOB books chunk {i}/{n_chunks}: cached ({len(cached)} books)")
                continue
            params = [BookParams(token_id=t) for t in chunk]
            async with self._limiter:
                books = await asyncio.to_thread(self._client.get_order_books, params)
            out.extend(books)
            if cache is not None:
                cache.save_books_chunk(i, books)
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
