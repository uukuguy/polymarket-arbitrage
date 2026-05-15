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

    async def fetch_all_active_markets(self) -> list[dict]:
        """Paginate ``/markets`` — strip to the ~17 fields normalizer needs."""
        return await self._paginate(
            path="/markets",
            params={"active": "true", "closed": "false", "archived": "false"},
            label="markets",
            keep_fields=self._MARKET_KEEP,
        )

    async def fetch_all_active_events(self) -> list[dict]:
        """Paginate ``/events`` — strip to ~12 fields. Nested markets
        trimmed to ``[{"id": ...}]`` (only used for market→event mapping)."""
        raw = await self._paginate(
            path="/events",
            params={"active": "true", "closed": "false"},
            label="events",
            keep_fields=self._EVENT_KEEP,
        )
        # Trim nested markets to just id (normalizer only does m.get("id"))
        for ev in raw:
            markets = ev.get("markets")
            if isinstance(markets, list):
                ev["markets"] = [{"id": m.get("id")} for m in markets if isinstance(m, dict)]
        return raw

    async def _paginate(
        self,
        *,
        path: str,
        params: dict,
        label: str,
        keep_fields: frozenset[str] | None = None,
    ) -> list[dict]:
        """Shared pagination loop for /markets and /events.

        ``params`` is a base dict (filters); we layer ``limit`` + ``offset`` on
        each call. ``keep_fields``, when set, strips every dict to only those
        keys — Polymarket objects carry 50+ fields but normalizer uses ~15.
        """
        out: list[dict] = []
        offset = 0
        pages_fetched = 0
        PROGRESS_EVERY = 50

        logger.info(
            f"Gamma: starting paginated fetch of {label} (page_limit={self.PAGE_LIMIT})"
        )

        while True:
            page_params = {**params, "limit": self.PAGE_LIMIT, "offset": offset}
            try:
                page = await self._get(path, page_params)
            except _NonRetryableHTTPError as exc:
                # Polymarket 422 at offset>10000 (2026-05 cap). Return what
                # we already have instead of failing the entire snapshot.
                if "422" in str(exc) and out:
                    logger.warning(
                        f"Gamma {label}: 422 at offset={offset}, "
                        f"returning {len(out)} items fetched so far"
                    )
                    break
                raise
            if not isinstance(page, list):
                raise RuntimeError(
                    f"Gamma {path} returned {type(page).__name__}, expected list"
                )

            # Phase 02 Plan 01: stamp per-page fetch time on each raw dict.
            # The private key _page_fetched_at_ms carries the real per-page
            # timestamp through to normalize_market → page_fetched_at_ms column.
            # Using a private _ prefix to distinguish from Polymarket API fields.
            page_fetched_at_ms = int(time.time() * 1000)
            for raw in page:
                if isinstance(raw, dict):
                    raw["_page_fetched_at_ms"] = page_fetched_at_ms

            if keep_fields is not None:
                page = [
                    {k: v for k, v in raw.items() if k in keep_fields}
                    for raw in page
                    if isinstance(raw, dict)
                ]

            out.extend(page)
            pages_fetched += 1

            # Yield to event loop every page so uvicorn and health checks
            # can run. httpx HTTP/2 responses return in ~40ms — too fast
            # for asyncio cooperative scheduling to give other coroutines
            # enough cycles during 100-page paginated fetches.
            await asyncio.sleep(0)

            if pages_fetched == 1 or pages_fetched % PROGRESS_EVERY == 0:
                logger.info(
                    f"Gamma: {label} page {pages_fetched} fetched "
                    f"({len(out)} {label} so far)"
                )

            if len(page) < self.PAGE_LIMIT:
                break

            if pages_fetched >= self.MAX_PAGES:
                raise RuntimeError(
                    f"Gamma {label} pagination exceeded {self.MAX_PAGES} pages "
                    f"— possible runaway response"
                )

            offset += self.PAGE_LIMIT

        logger.info(
            f"Gamma fetched {len(out)} active {label} in {pages_fetched} pages (final)"
        )
        return out
