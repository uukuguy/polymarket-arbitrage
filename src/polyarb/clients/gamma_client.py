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

    async def fetch_all_active_markets(self) -> list[dict]:
        """Paginate ``/markets`` and return every active+open+non-archived market dict.

        Returns RAW dicts as Polymarket sent them — including the
        ``clobTokenIds`` JSON-string field (Pitfall 2; Plan 4 normalizes).

        Raises ``RuntimeError`` if pagination exceeds ``MAX_PAGES`` (F-2) or
        if a page is not a list (defensive — Polymarket should always
        return ``list[dict]`` here).
        """
        out: list[dict] = []
        offset = 0
        pages_fetched = 0

        while True:
            params = {
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": self.PAGE_LIMIT,
                "offset": offset,
            }
            page = await self._get("/markets", params)
            if not isinstance(page, list):
                raise RuntimeError(
                    f"Gamma /markets returned {type(page).__name__}, expected list"
                )

            out.extend(page)
            pages_fetched += 1

            # Short page → end of stream
            if len(page) < self.PAGE_LIMIT:
                break

            # F-2 SECURITY: ceiling check AFTER the increment. With PAGE_LIMIT=100
            # and MAX_PAGES=1000 this allows up to 100k markets before failing.
            if pages_fetched >= self.MAX_PAGES:
                raise RuntimeError(
                    f"Gamma pagination exceeded {self.MAX_PAGES} pages — possible runaway response"
                )

            offset += self.PAGE_LIMIT

        logger.info(f"Gamma fetched {len(out)} active markets in {pages_fetched} pages")
        return out
