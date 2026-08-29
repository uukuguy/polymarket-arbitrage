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
- malformed JSON raised at the httpx boundary is wrapped in a bounded,
  body-free ``GammaMalformedResponseError``. It is intentionally NOT in
  ``retry_if_exception_type``. Rationale:
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
from collections.abc import AsyncIterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast
from urllib.parse import quote

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

GAMMA_CLOSE_TIMEOUT_S = 2.0


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


@dataclass(frozen=True)
class EventPage:
    """One bounded Gamma event page with its opaque durable continuation."""

    events: tuple[dict, ...]
    requested_cursor: str | None
    next_cursor: str | None
    completed: bool
    started_at_ms: int
    finished_at_ms: int

    @property
    def event_ids(self) -> tuple[str, ...]:
        return tuple(str(event["id"]) for event in self.events)


@dataclass(frozen=True)
class MarketPage:
    """One bounded Gamma market page with its opaque durable continuation."""

    markets: tuple[dict, ...]
    requested_cursor: str | None
    next_cursor: str | None
    completed: bool
    started_at_ms: int
    finished_at_ms: int


@dataclass
class PaginationCoverage:
    source: str
    result: PaginationResult = field(default_factory=lambda: PaginationResult(0, 0, False, None))


class PaginationIntegrityError(RuntimeError):
    """Raised when Gamma cannot prove that keyset pagination completed."""


class PaginationCursorRejectedError(RuntimeError):
    """Gamma rejected a non-empty durable keyset continuation."""

    def __init__(self, source: str, status_code: int) -> None:
        self.source = source
        self.status_code = status_code
        super().__init__(f"{source}-cursor-rejected-{status_code}")


class GammaMalformedResponseError(RuntimeError):
    """A successful Gamma response could not be decoded as JSON.

    Only bounded transport metadata is retained.  Provider bodies can contain
    arbitrary upstream text and must never enter logs, incidents, or durable
    runtime evidence.
    """

    def __init__(self, *, status_code: int, content_type: str, body_bytes: int) -> None:
        self.status_code = status_code
        self.content_type = content_type.split(";", 1)[0].strip().casefold()
        self.body_bytes = body_bytes
        super().__init__(
            "gamma-malformed-json-response:"
            f"status={status_code}:content_type={self.content_type or 'missing'}:"
            f"body_bytes={body_bytes}"
        )


class GammaClient:
    """Async client for Polymarket's Gamma metadata REST API.

    Constructor takes a fully-built ``Settings`` (Plan 1) — no ad-hoc kwargs.
    Use as an async context manager OR call ``aclose()`` explicitly.
    """

    PAGE_LIMIT = 100
    # Point lookups reconcile the race window between durable event and market
    # traversals. Large sets use bounded exact-id batches rather than one HTTP
    # request per identity.
    MAX_MARKET_STATE_LOOKUPS = 1_000
    MARKET_STATE_POINT_LOOKUP_LIMIT = 100
    MARKET_STATE_LOOKUP_BATCH_SIZE = 25
    # Gamma can leave a larger stale tail in the active market keyset than the
    # active event keyset. Batch exact-id enrichment bounds both fan-out and
    # individual request size.
    MAX_MARKET_PARENT_LOOKUPS = 500
    MARKET_PARENT_LOOKUP_BATCH_SIZE = 25
    # Parent enrichment is a bounded reconciliation side path.  Four parallel
    # exact-id batches stay far below Gamma's published request budget while
    # preventing one slow batch per 25 rows from serially consuming the whole
    # Structure deadline.
    MAX_CONCURRENT_PARENT_LOOKUPS = 4
    # F-2 SECURITY: ceiling on pagination loop (100k markets is far above any
    # realistic Polymarket size). See module docstring.
    MAX_PAGES = 1000

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # aiolimiter: token bucket. Polymarket Gamma published limit is ~300/10s;
        # we configure 280/10s as a conservative floor.
        self._limiter = AsyncLimiter(settings.gamma_rate_per_10s, 10)
        self._http = self._build_http_client()

    def _build_http_client(self) -> httpx.AsyncClient:
        """Create one replaceable HTTP transport generation."""
        return httpx.AsyncClient(
            timeout=self._settings.http_timeout_s,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            headers={"User-Agent": "polyarb/0.1"},
            http2=True,
            # F-2 SECURITY: explicit even though it's httpx's current default.
            follow_redirects=False,
        )

    async def reset_transport(self) -> None:
        """Retire a failed HTTP generation without resetting rate-limit state.

        Closing the old pool is cleanup, not another attempt deadline. A close
        timeout or close error cannot prevent installation of the fresh pool.
        """
        failed_http = self._http
        try:
            await asyncio.wait_for(
                failed_http.aclose(),
                timeout=GAMMA_CLOSE_TIMEOUT_S,
            )
        except TimeoutError:
            logger.warning("gamma transport generation close timed out during replacement")
        except Exception as error:
            logger.warning(
                "gamma transport generation close failed during replacement: {}",
                type(error).__name__,
            )
        finally:
            self._http = self._build_http_client()

    async def aclose(self) -> None:
        """Close the HTTP client without letting cleanup own process lifetime."""
        try:
            await asyncio.wait_for(
                self._http.aclose(),
                timeout=GAMMA_CLOSE_TIMEOUT_S,
            )
        except TimeoutError:
            logger.warning("gamma transport close timed out during cleanup")
        except Exception as error:
            logger.warning(
                "gamma transport close failed during cleanup: {}",
                type(error).__name__,
            )

    async def __aenter__(self) -> GammaClient:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
            try:
                await asyncio.wait_for(
                    self.aclose(),
                    timeout=GAMMA_CLOSE_TIMEOUT_S,
                )
            except TimeoutError:
                # A bounded Structure child is about to report its real page
                # deadline. Do not let a transport close wait consume the
                # remaining parent process envelope.
                logger.warning("gamma client close timed out after cancellation")
            return None
        await self.aclose()

    async def _get(
        self,
        path: str,
        params: Mapping[str, object] | Sequence[tuple[str, str]],
    ) -> list[dict] | dict:
        """Single GET with retry policy.

        Retries on transient failures (network, timeout, 5xx, 429) up to
        ``settings.retry_attempts`` times with exponential backoff.

        Does NOT retry:
        - 4xx other than 429 (raised as ``_NonRetryableHTTPError``)
        - malformed JSON (wrapped without the provider body)
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
                    r = await self._http.get(url, params=cast(Any, params))
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
                    try:
                        return r.json()
                    except ValueError as error:
                        raise GammaMalformedResponseError(
                            status_code=r.status_code,
                            content_type=r.headers.get("content-type", ""),
                            body_bytes=len(r.content),
                        ) from error

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

    async def iter_active_markets(self, coverage: PaginationCoverage) -> AsyncIterator[dict]:
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

    async def fetch_active_market_page(
        self,
        cursor: str | None,
        limit: int,
    ) -> MarketPage:
        """Fetch exactly one validated market page for durable Structure sync."""
        if type(limit) is not int or not 1 <= limit <= self.PAGE_LIMIT:
            raise PaginationIntegrityError(
                "/markets/keyset page limit must be within 1..100"
            )
        started_at_ms = int(time.time() * 1_000)
        markets, next_cursor, completed, finished_at_ms = await self._fetch_keyset_page(
            path="/markets/keyset",
            array_key="markets",
            params={"active": "true", "closed": "false", "archived": "false"},
            keep_fields=self._MARKET_KEEP,
            cursor=cursor,
            limit=limit,
        )
        return MarketPage(
            markets=markets,
            requested_cursor=cursor,
            next_cursor=next_cursor,
            completed=completed,
            started_at_ms=started_at_ms,
            finished_at_ms=finished_at_ms,
        )

    async def fetch_markets_by_ids(self, market_ids: tuple[str, ...]) -> tuple[dict, ...]:
        """Fetch one immutable, exact market-ID batch for a scoped source job."""
        if not market_ids or len(market_ids) > self.MARKET_STATE_LOOKUP_BATCH_SIZE:
            raise PaginationIntegrityError("exact-id market batch must contain 1..25 IDs")
        if tuple(sorted(market_ids)) != market_ids or len(set(market_ids)) != len(market_ids):
            raise PaginationIntegrityError("exact-id market batch must be sorted and unique")
        if any(not isinstance(market_id, str) or not market_id.strip() for market_id in market_ids):
            raise PaginationIntegrityError("exact-id market batch has invalid identity")
        payload = await self._get(
            "/markets",
            [("id", market_id) for market_id in market_ids] + [("limit", str(len(market_ids)))],
        )
        if not isinstance(payload, list) or not all(isinstance(market, dict) for market in payload):
            raise PaginationIntegrityError("exact-id market response has invalid shape")
        response_ids = [market.get("id") for market in payload]
        if (
            len(response_ids) != len(set(response_ids))
            or any(not isinstance(market_id, str) for market_id in response_ids)
            or set(response_ids) != set(market_ids)
        ):
            raise PaginationIntegrityError("exact-id market response identity set mismatch")
        by_id = {str(market["id"]): market for market in payload}
        for market_id in market_ids:
            market = by_id[market_id]
            if (
                market.get("active") is not True
                or market.get("closed") is not False
                or market.get("archived") is True
            ):
                raise PaginationIntegrityError("exact-id market response is not open")
        return tuple(by_id[market_id] for market_id in market_ids)

    async def fetch_market_states(
        self,
        market_ids: list[str],
    ) -> dict[str, dict[str, bool]]:
        """Point-check missing event members against authoritative market state.

        Calls are deduplicated, deterministic, and strictly bounded. Any
        malformed response raises so the snapshot remains fail-closed.
        """
        unique_ids = sorted(set(market_ids))
        if len(unique_ids) > self.MAX_MARKET_STATE_LOOKUPS:
            raise PaginationIntegrityError(
                "market state lookup limit exceeded: "
                f"{len(unique_ids)}>{self.MAX_MARKET_STATE_LOOKUPS}"
            )
        if any(type(market_id) is not str or not market_id.strip() for market_id in unique_ids):
            raise PaginationIntegrityError("market state lookup has invalid identity")

        if len(unique_ids) > self.MARKET_STATE_POINT_LOOKUP_LIMIT:
            async def fetch_batch(batch: list[str]) -> dict[str, dict[str, bool]]:
                payload = await self._get(
                    "/markets",
                    [("id", market_id) for market_id in batch]
                    + [("limit", str(len(batch)))],
                )
                if not isinstance(payload, list) or not all(
                    isinstance(market, dict) for market in payload
                ):
                    raise PaginationIntegrityError(
                        "/markets exact-id state response has invalid shape"
                    )
                response_ids = [market.get("id") for market in payload]
                if (
                    any(type(market_id) is not str for market_id in response_ids)
                    or len(response_ids) != len(set(response_ids))
                    or not set(response_ids) <= set(batch)
                ):
                    raise PaginationIntegrityError(
                        "/markets exact-id state response identity set mismatch"
                    )
                batch_states: dict[str, dict[str, bool]] = {}
                for market in payload:
                    active = market.get("active")
                    closed = market.get("closed")
                    if type(active) is not bool or type(closed) is not bool:
                        raise PaginationIntegrityError(
                            f"/markets?id={market.get('id')} state response has invalid state"
                        )
                    batch_states[market["id"]] = {
                        "active": active,
                        "closed": closed,
                    }
                for market_id in set(batch) - set(response_ids):
                    batch_states[market_id] = {
                        "active": False,
                        "closed": True,
                        "source_absent": True,
                    }
                return batch_states

            batches = [
                unique_ids[start : start + self.MARKET_STATE_LOOKUP_BATCH_SIZE]
                for start in range(0, len(unique_ids), self.MARKET_STATE_LOOKUP_BATCH_SIZE)
            ]
            states: dict[str, dict[str, bool]] = {}
            for start in range(0, len(batches), self.MAX_CONCURRENT_PARENT_LOOKUPS):
                wave = batches[start : start + self.MAX_CONCURRENT_PARENT_LOOKUPS]
                for batch_states in await asyncio.gather(
                    *(fetch_batch(batch) for batch in wave)
                ):
                    states.update(batch_states)
            return states

        states: dict[str, dict[str, bool]] = {}
        for market_id in unique_ids:
            used_fallback = False
            try:
                payload = await self._get(f"/markets/{quote(market_id, safe='')}", {})
            except _NonRetryableHTTPError as error:
                cause = error.__cause__
                if (
                    not isinstance(cause, httpx.HTTPStatusError)
                    or cause.response.status_code != 404
                ):
                    raise
                # Gamma's point route can carry a short-lived CDN negative
                # cache for a newly-created market while the exact-id list
                # route already has authoritative state. Only 404 activates
                # this fallback; every other error remains fail-closed.
                exact = await self._get(
                    "/markets",
                    [("id", market_id), ("limit", "1")],
                )
                if (
                    not isinstance(exact, list)
                    or len(exact) != 1
                    or not isinstance(exact[0], dict)
                    or exact[0].get("id") != market_id
                ):
                    raise PaginationIntegrityError("market state fallback identity set mismatch")
                payload = exact[0]
                used_fallback = True
            if not isinstance(payload, dict):
                raise PaginationIntegrityError(
                    f"/markets/{market_id} point response has invalid shape"
                )
            if payload.get("id") != market_id:
                raise PaginationIntegrityError(
                    f"/markets/{market_id} point response identity mismatch"
                )
            active = payload.get("active")
            closed = payload.get("closed")
            if type(active) is not bool or type(closed) is not bool:
                source = "fallback" if used_fallback else "point response"
                raise PaginationIntegrityError(f"/markets/{market_id} {source} has invalid state")
            states[market_id] = {"active": active, "closed": closed}
        return states

    async def fetch_market_parent_states(
        self,
        market_groups: dict[str, str],
    ) -> dict[str, dict[str, str | bool | None]]:
        """Resolve nested parent-event truth for unattached neg-risk markets.

        ``/markets/{id}`` omits nested events, while the exact-id list endpoint
        includes them. Every identity and state field is validated strictly;
        ambiguous or missing parents raise so callers cannot quarantine a live
        group on incomplete evidence.
        """
        items = sorted(market_groups.items())
        if len(items) > self.MAX_MARKET_PARENT_LOOKUPS:
            raise PaginationIntegrityError(
                "market parent lookup limit exceeded: "
                f"{len(items)}>{self.MAX_MARKET_PARENT_LOOKUPS}"
            )

        for market_id, expected_group_id in items:
            if (
                type(market_id) is not str
                or not market_id.strip()
                or type(expected_group_id) is not str
                or not expected_group_id.strip()
            ):
                raise PaginationIntegrityError("market parent lookup has invalid identity")

        async def fetch_batch(
            batch: list[tuple[str, str]],
        ) -> dict[str, dict[str, str | bool | None]]:
            expected_groups = dict(batch)
            expected_ids = set(expected_groups)
            payload = await self._get(
                "/markets",
                [("id", market_id) for market_id, _ in batch] + [("limit", str(len(batch)))],
            )
            if not isinstance(payload, list) or not all(
                isinstance(market, dict) for market in payload
            ):
                raise PaginationIntegrityError(
                    "/markets exact-id parent response has invalid shape"
                )
            response_ids = [market.get("id") for market in payload]
            if (
                any(type(market_id) is not str for market_id in response_ids)
                or len(response_ids) != len(set(response_ids))
                or not set(response_ids) <= expected_ids
            ):
                raise PaginationIntegrityError(
                    "/markets exact-id parent response identity set mismatch"
                )

            batch_states: dict[str, dict[str, str | bool | None]] = {}
            missing_ids = sorted(expected_ids - set(response_ids))
            if missing_ids:
                point_states = await self.fetch_market_states(missing_ids)
                for market_id in missing_ids:
                    state = point_states[market_id]
                    source_absent = state.get("source_absent") is True
                    active = state.get("active")
                    closed = state.get("closed")
                    if not source_absent and active is True and closed is False:
                        raise PaginationIntegrityError(
                            "/markets exact-id parent response omitted open market"
                        )
                    batch_states[market_id] = {
                        "event_id": None,
                        "active": bool(active),
                        "closed": bool(closed),
                        "archived": bool(source_absent or closed or not active),
                        "source_absent": source_absent,
                    }
            for market in payload:
                market_id = market["id"]
                if (
                    market.get("negRisk") is not True
                    or market.get("negRiskMarketID") != expected_groups[market_id]
                ):
                    raise PaginationIntegrityError(
                        f"/markets?id={market_id} parent response group mismatch"
                    )
                events = market.get("events")
                if (
                    not isinstance(events, list)
                    or len(events) != 1
                    or not isinstance(events[0], dict)
                ):
                    raise PaginationIntegrityError(
                        f"/markets?id={market_id} parent response is ambiguous"
                    )
                event = events[0]
                event_id = event.get("id")
                active = event.get("active")
                closed = event.get("closed")
                archived = event.get("archived")
                if type(event_id) is not str or not event_id.strip():
                    raise PaginationIntegrityError(
                        f"/markets?id={market_id} parent response has invalid identity"
                    )
                if any(type(value) is not bool for value in (active, closed, archived)):
                    raise PaginationIntegrityError(
                        f"/markets?id={market_id} parent response has invalid state"
                    )
                batch_states[market_id] = {
                    "event_id": event_id,
                    "active": active,
                    "closed": closed,
                    "archived": archived,
                }
            return batch_states

        batches = [
            items[start : start + self.MARKET_PARENT_LOOKUP_BATCH_SIZE]
            for start in range(0, len(items), self.MARKET_PARENT_LOOKUP_BATCH_SIZE)
        ]
        states: dict[str, dict[str, str | bool | None]] = {}
        for start in range(0, len(batches), self.MAX_CONCURRENT_PARENT_LOOKUPS):
            wave = batches[start : start + self.MAX_CONCURRENT_PARENT_LOOKUPS]
            for batch_states in await asyncio.gather(*(fetch_batch(batch) for batch in wave)):
                states.update(batch_states)
        return states

    async def iter_active_events(self, coverage: PaginationCoverage) -> AsyncIterator[dict]:
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

    async def fetch_active_event_page(
        self,
        cursor: str | None,
        limit: int,
    ) -> EventPage:
        """Fetch exactly one validated keyset page.

        The continuation is an opaque upstream token.  This method never loops
        and therefore cannot silently become a universe-sized operation.
        """
        if type(limit) is not int or not 1 <= limit <= self.PAGE_LIMIT:
            raise PaginationIntegrityError(
                f"/events/keyset page limit must be within 1..{self.PAGE_LIMIT}"
            )
        started_at_ms = int(time.time() * 1_000)
        items, next_cursor, completed, finished_at_ms = await self._fetch_keyset_page(
            path="/events/keyset",
            array_key="events",
            params={"active": "true", "closed": "false"},
            keep_fields=self._EVENT_KEEP,
            cursor=cursor,
            limit=limit,
        )
        events = tuple(self._project_event(item) for item in items)
        return EventPage(
            events=events,
            requested_cursor=cursor,
            next_cursor=next_cursor,
            completed=completed,
            started_at_ms=started_at_ms,
            finished_at_ms=finished_at_ms,
        )

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
            f"Gamma: starting streaming fetch of {coverage.source} (page_limit={self.PAGE_LIMIT})"
        )
        while True:
            projected_items, next_cursor, completed, _ = await self._fetch_keyset_page(
                path=path,
                array_key=array_key,
                params=params,
                keep_fields=keep_fields,
                cursor=cursor,
                limit=self.PAGE_LIMIT,
            )
            pages += 1
            for projected in projected_items:
                items += 1
                yield projected
            await asyncio.sleep(0)
            if pages == 1 or pages % progress_every == 0:
                logger.info(
                    f"Gamma: {coverage.source} page {pages} fetched "
                    f"({items} {coverage.source} so far)"
                )
            if completed:
                coverage.result = PaginationResult(items, pages, True, None)
                logger.info(
                    f"Gamma streamed {items} active {coverage.source} in {pages} pages (final)"
                )
                return
            if next_cursor in seen:
                coverage.result = PaginationResult(items, pages, False, cursor)
                raise PaginationIntegrityError(f"{path} repeated cursor")
            assert next_cursor is not None
            seen.add(next_cursor)
            cursor = next_cursor
            if pages >= self.MAX_PAGES:
                coverage.result = PaginationResult(items, pages, False, cursor)
                raise PaginationIntegrityError(f"{path} exceeded {self.MAX_PAGES} pages")

    async def _fetch_keyset_page(
        self,
        *,
        path: str,
        array_key: str,
        params: dict[str, str],
        keep_fields: frozenset[str],
        cursor: str | None,
        limit: int,
    ) -> tuple[tuple[dict, ...], str | None, bool, int]:
        """Shared shape/cursor validation for bounded and streaming callers."""
        request_params = {**params, "limit": str(limit)}
        if cursor is not None:
            request_params["after_cursor"] = cursor
        try:
            payload = await self._get(path, request_params)
        except _NonRetryableHTTPError as error:
            cause = error.__cause__
            status_code = (
                cause.response.status_code
                if isinstance(cause, httpx.HTTPStatusError)
                else None
            )
            if cursor is not None and status_code in {400, 403}:
                raise PaginationCursorRejectedError(
                    array_key,
                    status_code,
                ) from error
            raise
        if not isinstance(payload, dict) or not isinstance(payload.get(array_key), list):
            raise PaginationIntegrityError(f"{path} keyset response has invalid shape")
        page_fetched_at_ms = int(time.time() * 1_000)
        projected_items: list[dict] = []
        for raw in payload[array_key]:
            if not isinstance(raw, dict):
                if array_key == "events":
                    raise PaginationIntegrityError(
                        f"{path} keyset member is not an object"
                    )
                continue
            if array_key == "events":
                identity = raw.get("id")
                if (
                    type(identity) is not str
                    or not identity
                    or identity != identity.strip()
                ):
                    raise PaginationIntegrityError(
                        f"{path} keyset member has invalid identity"
                    )
                if raw.get("active") is not True or raw.get("closed") is not False:
                    raise PaginationIntegrityError(
                        f"{path} keyset member has invalid state"
                    )
            raw = {**raw, "_page_fetched_at_ms": page_fetched_at_ms}
            projected = {
                key: value for key, value in raw.items() if key in keep_fields
            }
            if projected.get("active") is True and projected.get("closed") is not True:
                projected_items.append(projected)
        next_cursor = payload.get("next_cursor")
        if next_cursor in (None, ""):
            return tuple(projected_items), None, True, page_fetched_at_ms
        if not isinstance(next_cursor, str):
            raise PaginationIntegrityError(f"{path} invalid cursor")
        if next_cursor == cursor:
            raise PaginationIntegrityError(f"{path} repeated cursor")
        return tuple(projected_items), next_cursor, False, page_fetched_at_ms

    @staticmethod
    def _project_event(raw: dict) -> dict:
        projected = dict(raw)
        markets = projected.get("markets")
        if isinstance(markets, list):
            projected["markets"] = [
                (
                    {
                        "id": market.get("id"),
                        "conditionId": market.get("conditionId"),
                        "clobTokenIds": market.get("clobTokenIds"),
                        "outcomePrices": market.get("outcomePrices"),
                        "slug": market.get("slug"),
                        "question": market.get("question"),
                        "active": market.get("active"),
                        "closed": market.get("closed"),
                        "negRisk": market.get("negRisk"),
                        "liquidity": market.get("liquidity"),
                        "liquidityNum": market.get("liquidityNum"),
                        "volume": market.get("volume"),
                        "volumeNum": market.get("volumeNum"),
                        "endDate": market.get("endDate"),
                        "negRiskOther": market.get("negRiskOther"),
                        "groupItemTitle": market.get("groupItemTitle"),
                    }
                    if isinstance(market, dict)
                    else market
                )
                for market in markets
            ]
        return projected
