"""Async Polymarket Gamma metadata client.

Pattern: long-lived ``httpx.AsyncClient`` + ``aiolimiter`` (token bucket) +
``tenacity`` exponential backoff. Keyset pagination exposes an explicit
completion result so callers cannot confuse a truncated fetch with complete
market coverage.

F-2 SECURITY:
- ``follow_redirects=False`` is httpx's current default but pinned explicitly
  to prevent silent SSRF exposure if a future httpx default flips.
  Polymarket's CDN should never redirect us.
- ``MAX_PAGES = 1000`` caps pagination at 100k markets (Polymarket has ~20k
  active markets); a buggy or hostile endpoint that returns full pages forever
  will trigger a ``PaginationIntegrityError`` instead of OOMing.

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
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

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


@dataclass(frozen=True)
class PaginationResult:
    items_yielded: int
    pages_fetched: int
    completed: bool
    final_cursor: str | None


@dataclass
class PaginationCoverage:
    source: str
    result: PaginationResult = field(
        default_factory=lambda: PaginationResult(0, 0, False, None)
    )


class PaginationIntegrityError(RuntimeError):
    """Raised when Gamma cannot prove that keyset pagination completed."""


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
    _MARKET_KEEP = frozenset(
        {
            "id",
            "conditionId",
            "slug",
            "question",
            "clobTokenIds",
            "outcomePrices",
            "active",
            "closed",
            "negRisk",
            "negRiskMarketID",
            "liquidity",
            "liquidityNum",
            "volume",
            "volumeNum",
            "endDate",
            "end_date_iso",
            "_page_fetched_at_ms",
        }
    )
    _EVENT_KEEP = frozenset(
        {
            "id",
            "slug",
            "title",
            "ticker",
            "active",
            "closed",
            "negRisk",
            "enableNegRisk",
            "negRiskAugmented",
            "negRiskMarketID",
            "liquidity",
            "liquidityNum",
            "volume",
            "volumeNum",
            "endDate",
            "tags",
            "markets",
            "_page_fetched_at_ms",
        }
    )

    # Plan 02-09 (D-23): streaming primary API ───────────────────────────
    # iter_active_markets is the memory-bounded path the orchestrator uses;
    # fetch_all_active_markets is the backward-compat collector retained for
    # tests and one-off scripts. _paginate yields per-item (no accumulator).
    # TODO(02-09 follow-up): once orchestrator confirmed stable on streaming,
    # remove fetch_all_active_markets (events stays a list — Decision A).

    async def iter_active_markets(
        self, coverage: PaginationCoverage
    ) -> AsyncIterator[dict]:
        """Stream active markets and record whether keyset traversal completed."""
        async for raw in self._paginate_keyset(
            path="/markets/keyset",
            array_key="markets",
            params={"active": "true", "closed": "false", "archived": "false"},
            keep_fields=self._MARKET_KEEP,
            coverage=coverage,
        ):
            yield raw

    async def fetch_all_active_markets(self) -> list[dict]:
        """Backward-compat: collect ``iter_active_markets`` into a list.

        DEPRECATED for hot paths — prefer ``iter_active_markets`` to avoid the
        full in-memory list. Retained for tests and one-off scripts.
        """
        coverage = PaginationCoverage(source="markets")
        return [m async for m in self.iter_active_markets(coverage)]

    async def iter_active_events(
        self, coverage: PaginationCoverage
    ) -> AsyncIterator[dict]:
        """Stream active events and record whether keyset traversal completed."""
        async for raw in self._paginate_keyset(
            path="/events/keyset",
            array_key="events",
            params={"active": "true", "closed": "false"},
            keep_fields=self._EVENT_KEEP,
            coverage=coverage,
        ):
            markets = raw.get("markets")
            if isinstance(markets, list):
                raw["markets"] = [
                    (
                        {
                            "id": market.get("id"),
                            "active": market.get("active"),
                            "closed": market.get("closed"),
                            "negRiskOther": market.get("negRiskOther"),
                            "groupItemTitle": market.get("groupItemTitle"),
                        }
                        if isinstance(market, dict)
                        else market
                    )
                    for market in markets
                ]
            yield raw

    async def fetch_all_active_events(self) -> list[dict]:
        """Backward-compat: collect ``iter_active_events`` into a list.

        Decision A (Plan 02-09): events stays materialized because
        ``normalize_events`` builds a map the markets pass depends on. This
        wrapper preserves the existing orchestrator call site.
        """
        coverage = PaginationCoverage(source="events")
        return [e async for e in self.iter_active_events(coverage)]

    async def _paginate_keyset(
        self,
        *,
        path: str,
        array_key: str,
        params: dict[str, str],
        keep_fields: frozenset[str],
        coverage: PaginationCoverage,
    ) -> AsyncIterator[dict]:
        """Stream a Gamma keyset while maintaining explicit completion proof."""
        cursor: str | None = None
        seen: set[str] = set()
        items = pages = 0
        progress_every = 50
        logger.info(
            f"Gamma: starting streaming fetch of {coverage.source} "
            f"(page_limit={self.PAGE_LIMIT})"
        )
        while True:
            request_params = {**params, "limit": str(self.PAGE_LIMIT)}
            if cursor is not None:
                request_params["after_cursor"] = cursor
            payload = await self._get(path, request_params)
            if not isinstance(payload, dict) or not isinstance(payload.get(array_key), list):
                raise PaginationIntegrityError(f"{path} keyset response has invalid shape")
            pages += 1
            page_fetched_at_ms = int(time.time() * 1000)
            for raw in payload[array_key]:
                if not isinstance(raw, dict):
                    continue
                raw["_page_fetched_at_ms"] = page_fetched_at_ms
                projected = {key: value for key, value in raw.items() if key in keep_fields}
                if projected.get("active") is True and projected.get("closed") is not True:
                    items += 1
                    yield projected
            await asyncio.sleep(0)
            if pages == 1 or pages % progress_every == 0:
                logger.info(
                    f"Gamma: {coverage.source} page {pages} fetched "
                    f"({items} {coverage.source} so far)"
                )
            next_cursor = payload.get("next_cursor")
            if next_cursor in (None, ""):
                coverage.result = PaginationResult(items, pages, True, None)
                logger.info(
                    f"Gamma streamed {items} active {coverage.source} "
                    f"in {pages} pages (final)"
                )
                return
            if not isinstance(next_cursor, str) or next_cursor in seen:
                coverage.result = PaginationResult(items, pages, False, cursor)
                raise PaginationIntegrityError(f"{path} repeated cursor")
            seen.add(next_cursor)
            cursor = next_cursor
            if pages >= self.MAX_PAGES:
                coverage.result = PaginationResult(items, pages, False, cursor)
                raise PaginationIntegrityError(f"{path} exceeded {self.MAX_PAGES} pages")
