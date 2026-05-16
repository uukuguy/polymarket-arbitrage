"""Async Polymarket Gamma metadata client.

Pattern: long-lived ``httpx.AsyncClient`` + ``aiolimiter`` (token bucket) +
``tenacity`` exponential backoff. Returns RAW dicts verbatim — normalization
is owned by Plan 4 (orchestrator).

F-2 SECURITY:
- ``follow_redirects=False`` is httpx's current default but pinned explicitly
  to prevent silent SSRF exposure if a future httpx default flips.
  Polymarket's CDN should never redirect us.
- ``MAX_PAGES = 1000`` caps pagination at 100k markets (Polymarket has ~20k
  active markets); a buggy or hostile endpoint that returns full pages forever
  will trigger a ``RuntimeError`` instead of OOMing.

F-6 SECURITY (NON-RETRY POLICY):
- ``json.JSONDecodeError`` raised at the httpx boundary is intentionally
  NOT in ``retry_if_exception_type`` and propagates directly. Rationale:
  a 200 with malformed JSON usually indicates CDN/cache misconfiguration,
  not transient network — retrying is unlikely to help and burns time.
  The orchestrator (Plan 4) categorizes the propagated exception as
  ``API_UNREACHABLE``.
- 4xx (other than 429) is NOT retried: these are caller errors and a retry
  cannot fix a bad request. We classify via ``_NonRetryableHTTPError`` so
  tenacity sees an exception that is NOT in its retry whitelist.

Anti-patterns deliberately avoided:
- NO per-call ``async with httpx.AsyncClient()`` (defeats keepalive / HTTP2)
- NO bare ``except Exception: log; return None`` (swallows real errors)
- NO ``Optional[X]`` (Python 3.12 ``X | None`` only)
"""

from __future__ import annotations

import asyncio
import time
from typing import AsyncIterator

import httpx
from aiolimiter import AsyncLimiter
from loguru import logger
from tenacity import (
    AsyncRetrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from polyarb.config import Settings


class _NonRetryableHTTPError(Exception):
    """Wraps a 4xx (non-429) httpx.HTTPStatusError so tenacity does NOT retry it.

    The original ``httpx.HTTPStatusError`` is preserved on ``__cause__`` so
    callers can inspect ``.response.status_code`` after unwrapping.
    """


class GammaClient:
    """Async client for Polymarket's Gamma metadata REST API.

    Constructor takes a fully-built ``Settings`` (Plan 1) — no ad-hoc kwargs.
    Use as an async context manager OR call ``aclose()`` explicitly.
    """

    PAGE_LIMIT = 100
    # F-2 SECURITY: ceiling on pagination loop (100k markets is far above any
    # realistic Polymarket size). See module docstring.
    MAX_PAGES = 1000

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # aiolimiter: token bucket. Polymarket Gamma published limit is ~300/10s;
        # we configure 280/10s as a conservative floor.
        self._limiter = AsyncLimiter(settings.gamma_rate_per_10s, 10)
        self._http = httpx.AsyncClient(
            timeout=settings.http_timeout_s,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            headers={"User-Agent": "polyarb/0.1"},
            http2=True,
            # F-2 SECURITY: explicit even though it's httpx's current default.
            follow_redirects=False,
        )

    async def aclose(self) -> None:
        """Close the underlying HTTP client (idempotent)."""
        await self._http.aclose()

    async def __aenter__(self) -> GammaClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def _get(self, path: str, params: dict) -> list[dict] | dict:
        """Single GET with retry policy.

        Retries on transient failures (network, timeout, 5xx, 429) up to
        ``settings.retry_attempts`` times with exponential backoff.

        Does NOT retry:
        - 4xx other than 429 (raised as ``_NonRetryableHTTPError``)
        - ``json.JSONDecodeError`` (propagates — orchestrator classifies)
        """
        s = self._settings
        url = f"{s.gamma_url}{path}"

        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(s.retry_attempts),
            wait=wait_exponential(multiplier=1, min=s.retry_min_wait_s, max=s.retry_max_wait_s),
            retry=retry_if_exception_type(
                (httpx.RequestError, httpx.HTTPStatusError, httpx.TimeoutException)
            ),
            reraise=True,
        ):
            with attempt:
                async with self._limiter:
                    r = await self._http.get(url, params=params)
                    # Pre-classify 4xx (non-429) so tenacity does NOT retry them.
                    if 400 <= r.status_code < 500 and r.status_code != 429:
                        try:
                            r.raise_for_status()
                        except httpx.HTTPStatusError as e:
                            # Re-raise as a non-retryable type (not in retry_if_exception_type).
                            raise _NonRetryableHTTPError(
                                f"non-retryable {r.status_code} from {url}"
                            ) from e
                    r.raise_for_status()
                    return r.json()

        # Unreachable: AsyncRetrying with reraise=True will raise from .with()
        # block above. Mypy/pyright happiness only.
        raise RuntimeError("AsyncRetrying exited without yielding — unreachable")

    # Fields the normalizer actually reads — everything else is dead weight
    # in memory. Polymarket events carry 50+ fields per object including
    # multi-KB description text and nested arrays we never use.
    _MARKET_KEEP = frozenset({
        "id", "conditionId", "slug", "question", "clobTokenIds",
        "outcomePrices", "active", "closed", "negRisk", "negRiskMarketID",
        "liquidity", "liquidityNum", "volume", "volumeNum",
        "endDate", "end_date_iso", "_page_fetched_at_ms",
    })
    _EVENT_KEEP = frozenset({
        "id", "slug", "title", "ticker", "active", "closed",
        "liquidity", "liquidityNum", "volume", "volumeNum",
        "endDate", "tags", "markets", "_page_fetched_at_ms",
    })

    # Plan 02-09 (D-23): streaming primary API ───────────────────────────
    # iter_active_markets is the memory-bounded path the orchestrator uses;
    # fetch_all_active_markets is the backward-compat collector retained for
    # tests and one-off scripts. _paginate yields per-item (no accumulator).
    # TODO(02-09 follow-up): once orchestrator confirmed stable on streaming,
    # remove fetch_all_active_markets (events stays a list — Decision A).

    async def iter_active_markets(self) -> AsyncIterator[dict]:
        """Stream ``/markets`` one stripped dict at a time. Memory invariant:
        paginator internal state is bounded by one Gamma page (~100 dicts) plus
        running counters — no accumulator."""
        async for raw in self._paginate(
            path="/markets",
            params={"active": "true", "closed": "false", "archived": "false"},
            label="markets",
            keep_fields=self._MARKET_KEEP,
        ):
            yield raw

    async def fetch_all_active_markets(self) -> list[dict]:
        """Backward-compat: collect ``iter_active_markets`` into a list.

        DEPRECATED for hot paths — prefer ``iter_active_markets`` to avoid the
        full in-memory list. Retained for tests and one-off scripts.
        """
        return [m async for m in self.iter_active_markets()]

    async def iter_active_events(self) -> AsyncIterator[dict]:
        """Stream ``/events`` one stripped dict at a time. Markets nested-list
        trimming is done eagerly here (per-event, not at end of stream)."""
        async for raw in self._paginate(
            path="/events",
            params={"active": "true", "closed": "false"},
            label="events",
            keep_fields=self._EVENT_KEEP,
        ):
            markets = raw.get("markets")
            if isinstance(markets, list):
                raw["markets"] = [
                    {"id": m.get("id")} for m in markets if isinstance(m, dict)
                ]
            yield raw

    async def fetch_all_active_events(self) -> list[dict]:
        """Backward-compat: collect ``iter_active_events`` into a list.

        Decision A (Plan 02-09): events stays materialized because
        ``normalize_events`` builds a map the markets pass depends on. This
        wrapper preserves the existing orchestrator call site.
        """
        return [e async for e in self.iter_active_events()]

    async def _paginate(
        self,
        *,
        path: str,
        params: dict,
        label: str,
        keep_fields: frozenset[str] | None = None,
    ) -> AsyncIterator[dict]:
        """Paginate ``path`` and YIELD individual dicts one at a time.

        Memory-bounded: internal state is one page of ~100 dicts plus running
        counters. No accumulator. Caller is responsible for buffering if a list
        is needed.

        Yields each filtered dict (with ``_page_fetched_at_ms`` stamped). Honors
        ``MAX_PAGES``, 422-offset-cap graceful stop, and all retry semantics
        from ``_get``.
        """
        offset = 0
        pages_fetched = 0
        items_yielded = 0
        PROGRESS_EVERY = 50

        logger.info(
            f"Gamma: starting streaming fetch of {label} (page_limit={self.PAGE_LIMIT})"
        )

        while True:
            page_params = {**params, "limit": self.PAGE_LIMIT, "offset": offset}
            try:
                page = await self._get(path, page_params)
            except _NonRetryableHTTPError as exc:
                # Polymarket 422 at offset>10000 (2026-05 cap). Stop cleanly
                # instead of failing the entire snapshot.
                if "422" in str(exc) and items_yielded > 0:
                    logger.warning(
                        f"Gamma {label}: 422 at offset={offset}, "
                        f"stopping at {items_yielded} items yielded so far"
                    )
                    return
                raise
            if not isinstance(page, list):
                raise RuntimeError(
                    f"Gamma {path} returned {type(page).__name__}, expected list"
                )

            # Phase 02 Plan 01: stamp per-page fetch time on each raw dict.
            page_fetched_at_ms = int(time.time() * 1000)
            page_size = len(page)
            for raw in page:
                if not isinstance(raw, dict):
                    continue
                raw["_page_fetched_at_ms"] = page_fetched_at_ms
                if keep_fields is not None:
                    raw = {k: v for k, v in raw.items() if k in keep_fields}
                yield raw
                items_yielded += 1

            pages_fetched += 1

            # Yield to event loop every page so uvicorn and health checks can run.
            await asyncio.sleep(0)

            if pages_fetched == 1 or pages_fetched % PROGRESS_EVERY == 0:
                logger.info(
                    f"Gamma: {label} page {pages_fetched} fetched "
                    f"({items_yielded} {label} so far)"
                )

            # Short-page terminate check uses the raw response length, not
            # `items_yielded` (page_size includes non-dict entries that get
            # filtered above; pagination contract is length-based).
            if page_size < self.PAGE_LIMIT:
                break

            if pages_fetched >= self.MAX_PAGES:
                raise RuntimeError(
                    f"Gamma {label} pagination exceeded {self.MAX_PAGES} pages "
                    f"— possible runaway response"
                )

            offset += self.PAGE_LIMIT

        logger.info(
            f"Gamma streamed {items_yielded} active {label} in {pages_fetched} pages (final)"
        )
