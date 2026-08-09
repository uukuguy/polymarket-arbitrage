"""Read-only production surface for M1→M2 opportunity discovery."""

from __future__ import annotations

import asyncio
import sqlite3
import time
from contextvars import ContextVar
from math import isfinite
from pathlib import Path

from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.http.health import MarketTruthHealth, read_market_truth_health
from polyarb.http.market_map import durable_opportunity_ids
from polyarb.http.opportunity_read_health import (
    OpportunityReadHealth,
    ReadLaneSaturatedError,
)
from polyarb.routing.feed_handoff import (
    FeedAvailability,
    decide_feed_availability,
)
from polyarb.routing.neg_risk_quote_store import QuoteUniverseUnavailableError
from polyarb.routing.opportunity_scanner import (
    QUOTE_SLA_SECONDS,
    UNIVERSE_SLA_SECONDS,
    QuoteRunUnavailableError,
    StaleQuoteRunError,
    StaleUniverseError,
)

_SOURCE_TRUTH_READ_TIMEOUT_S = 1.0
_LIFECYCLE_READ_TIMEOUT_S = 1.0
_ENDPOINT_TIMEOUT_S = 3.0
# The live 39k-token certified projection takes about 2.2s to reconstruct on
# Fly's attached volume. Keep its cold-cache read bounded below the endpoint's
# three-second absolute budget without rejecting every healthy restart.
_FEED_HYDRATION_TIMEOUT_S = 2.5
_SOURCE_TRUTH_READ_MODE: ContextVar[str] = ContextVar(
    "opportunity_source_truth_read_mode",
    default="legacy",
)


def _is_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _market_truth(db_path: Path, now_s: float) -> MarketTruthHealth:
    return read_market_truth_health(
        db_path,
        now_s,
        structure_generation_read_mode=_SOURCE_TRUTH_READ_MODE.get(),
    )


def _read_health(request: Request) -> OpportunityReadHealth:
    return request.app.state.opportunity_read_health


def _error_kind(error: BaseException) -> str:
    if isinstance(error, TimeoutError):
        return "timeout"
    if isinstance(error, ReadLaneSaturatedError):
        return "saturated"
    if isinstance(error, QuoteUniverseUnavailableError):
        return "source-truth-unavailable"
    return type(error).__name__


def _authenticated_fallback_truth(
    feed,
    *,
    result,
    quote_age_seconds: float,
    universe_age_seconds: float,
) -> MarketTruthHealth | None:
    """Rebuild only the exact immutable identity sealed into certification."""
    projection = feed.projection
    if (
        projection.universe_snapshot_id != result.source_snapshot_id
        or projection.run_id != result.quote_run_id
        or projection.universe_hash != result.universe_hash
        or not _is_sha256(projection.universe_hash)
        or not _is_sha256(projection.source_truth_hash)
        or quote_age_seconds > QUOTE_SLA_SECONDS
        or universe_age_seconds > UNIVERSE_SLA_SECONDS
    ):
        return None
    return MarketTruthHealth(
        coverage_status="warn",
        coverage_value="last-known-authenticated",
        latest_attempt_snapshot_id=projection.universe_snapshot_id,
        latest_attempt_market_items=None,
        latest_attempt_event_items=None,
        last_complete_snapshot_id=projection.universe_snapshot_id,
        last_complete_age_seconds=universe_age_seconds,
        last_complete_finished_age_seconds=universe_age_seconds,
    )


def _select_cached_opportunities(
    feed,
    *,
    min_edge_bps: float,
    limit: int,
    now_s: float,
):
    projection = feed.projection
    result = feed.opportunity_scan
    if result is None:
        raise QuoteRunUnavailableError("quote run unavailable")
    quote_age_seconds = max(0.0, now_s - projection.quoted_at_ms / 1000)
    universe_age_seconds = max(
        0.0,
        now_s - projection.universe_taken_at_ms / 1000,
    )
    selected = tuple(
        item
        for item in result.opportunities
        if item.gross_edge_bps >= min_edge_bps
    )[:limit]
    return result, selected, quote_age_seconds, universe_age_seconds


def _require_available_feed(
    availability: FeedAvailability,
    *,
    quote_age_seconds: float,
    universe_age_seconds: float,
) -> None:
    if availability.available:
        return
    if availability.reason == "stale-quote":
        raise StaleQuoteRunError(
            f"quote age {quote_age_seconds:.1f}s exceeds {QUOTE_SLA_SECONDS:.1f}s"
        )
    if availability.reason == "stale-universe":
        raise StaleUniverseError(
            f"universe age {universe_age_seconds:.1f}s exceeds {UNIVERSE_SLA_SECONDS:.1f}s"
        )
    raise QuoteUniverseUnavailableError(availability.reason or "feed-unavailable")


async def _opportunities(request: Request) -> JSONResponse:
    try:
        min_edge_bps = float(request.query_params.get("min_edge_bps", "0"))
        limit = min(100, max(1, int(request.query_params.get("limit", "20"))))
        if not isfinite(min_edge_bps) or min_edge_bps < 0:
            raise ValueError
    except ValueError:
        return JSONResponse({"error": "invalid numeric query"}, status_code=400)
    runtime = getattr(request.app.state, "quote_worker_runtime", None)
    feed = (
        runtime.certified_feed()
        if runtime is not None
        else None
    )
    # In the supervised topology, the Quote producer is deliberately isolated
    # from the HTTP parent. Hydrate only its already-certified durable result;
    # this request never performs CLOB collection or structure mutation.
    if feed is None:
        loader = getattr(request.app.state, "quote_feed_loader", None)
        if loader is not None:
            try:
                feed = await asyncio.wait_for(
                    asyncio.to_thread(loader),
                    timeout=_FEED_HYDRATION_TIMEOUT_S,
                )
            except (StaleQuoteRunError, StaleUniverseError) as error:
                # A cold HTTP parent must surface the already-classified feed
                # staleness as an availability response, never leak it through
                # Starlette as an ASGI 500 traceback.
                return JSONResponse({"error": str(error)}, status_code=503)
            except (TimeoutError, sqlite3.Error, ValueError):
                feed = None
            if feed is not None and runtime is not None:
                runtime.restore_certified_feed(feed)
    if feed is None or feed.opportunity_scan is None:
        return JSONResponse(
            {"error": "verified market universe unavailable"},
            status_code=503,
        )
    health = _read_health(request)
    now_s = time.time()
    try:
        result, selected, quote_age_seconds, universe_age_seconds = _select_cached_opportunities(
            feed,
            min_edge_bps=min_edge_bps,
            limit=limit,
            now_s=now_s,
        )
        source_truth_status = "live"
        source_truth_live_available = True
        source_truth_error_kind: str | None = None
        _require_available_feed(
            decide_feed_availability(
                source_snapshot_id=feed.projection.universe_snapshot_id,
                latest_structure_snapshot_id=feed.projection.universe_snapshot_id,
                quote_age_seconds=quote_age_seconds,
                universe_age_seconds=universe_age_seconds,
                handoff_age_seconds=0.0,
            ),
            quote_age_seconds=quote_age_seconds,
            universe_age_seconds=universe_age_seconds,
        )
        source_token = health.begin_source_attempt(now_s)
        try:
            mode_token = _SOURCE_TRUTH_READ_MODE.set(
                str(
                    getattr(
                        request.app.state.settings,
                        "structure_generation_read_mode",
                        "legacy",
                    )
                )
            )
            try:
                market_truth = await request.app.state.opportunity_source_truth_lane.run(
                    _market_truth,
                    request.app.state.sqlite_store.db_path,
                    now_s,
                    timeout_s=_SOURCE_TRUTH_READ_TIMEOUT_S,
                )
            finally:
                _SOURCE_TRUTH_READ_MODE.reset(mode_token)
            if (
                market_truth.last_complete_snapshot_id is None
                or getattr(market_truth, "coverage_status", "pass") == "fail"
            ):
                raise QuoteUniverseUnavailableError("source-truth-unavailable")
            health.mark_source_live(source_token, time.time())
        except Exception as error:  # noqa: BLE001 - certified fallback is fail-soft
            source_truth_status = "last-known-authenticated"
            source_truth_live_available = False
            source_truth_error_kind = _error_kind(error)
            market_truth = _authenticated_fallback_truth(
                feed,
                result=result,
                quote_age_seconds=quote_age_seconds,
                universe_age_seconds=universe_age_seconds,
            )
            if market_truth is None:
                health.mark_source_unavailable(
                    source_token,
                    time.time(),
                    source_truth_error_kind,
                    authentication_invalid=True,
                )
                raise QuoteUniverseUnavailableError("source-truth-unavailable") from error
            health.mark_source_fallback(
                source_token,
                time.time(),
                source_truth_error_kind,
            )
        availability = decide_feed_availability(
            source_snapshot_id=feed.projection.universe_snapshot_id,
            latest_structure_snapshot_id=market_truth.last_complete_snapshot_id,
            quote_age_seconds=quote_age_seconds,
            universe_age_seconds=universe_age_seconds,
            handoff_age_seconds=(
                market_truth.last_complete_finished_age_seconds
            ),
        )
        _require_available_feed(
            availability,
            quote_age_seconds=quote_age_seconds,
            universe_age_seconds=universe_age_seconds,
        )
        lifecycle_status = "pending"
        lifecycle_error_kind: str | None = None
        lifecycle_token = health.begin_lifecycle_attempt(time.time())
        try:
            durable_ids = await request.app.state.opportunity_lifecycle_lane.run(
                durable_opportunity_ids,
                request.app.state.sqlite_store.db_path,
                {item.group_id for item in selected},
                timeout_s=_LIFECYCLE_READ_TIMEOUT_S,
                structure_revision=result.source_snapshot_id,
                quote_run_id=result.quote_run_id,
                now_ms=int(now_s * 1000),
                quote_max_age_s=float(
                    request.app.state.settings.neg_risk_quote_interval_s
                ),
                structure_generation_read_mode=(
                    request.app.state.settings.structure_generation_read_mode
                ),
            )
            health.mark_lifecycle(
                lifecycle_token,
                time.time(),
                "available",
                None,
            )
        except Exception as error:  # noqa: BLE001 - lifecycle IDs are an attachment
            durable_ids = {}
            lifecycle_status = "unavailable"
            lifecycle_error_kind = _error_kind(error)
            health.mark_lifecycle(
                lifecycle_token,
                time.time(),
                lifecycle_status,
                lifecycle_error_kind,
            )
    except (QuoteUniverseUnavailableError, QuoteRunUnavailableError):
        return JSONResponse(
            {"error": "verified market universe unavailable"},
            status_code=503,
        )
    except StaleQuoteRunError as error:
        return JSONResponse({"error": str(error)}, status_code=503)
    except StaleUniverseError as error:
        return JSONResponse({"error": str(error)}, status_code=503)
    except (sqlite3.Error, TimeoutError, ValueError):
        return JSONResponse(
            {"error": "verified market universe unavailable"},
            status_code=503,
        )
    return JSONResponse(
        {
            "strategy": "neg-risk-buy-all",
            "profit_basis": "gross-before-fees",
            "coverage": "verified-standard-neg-risk",
            "refreshing": availability.refreshing,
            "source_truth_status": source_truth_status,
            "source_truth_live_available": source_truth_live_available,
            "latest_structure_snapshot_id": market_truth.last_complete_snapshot_id,
            "source_snapshot_id": result.source_snapshot_id,
            "universe_hash": result.universe_hash,
            "quote_run_id": result.quote_run_id,
            "quote_sla_seconds": QUOTE_SLA_SECONDS,
            "count": len(selected),
            "rejections": dict(result.rejections),
            "read_diagnostics": {
                "source_truth_status": source_truth_status,
                "source_truth_error_kind": source_truth_error_kind,
                "lifecycle_status": lifecycle_status,
                "lifecycle_error_kind": lifecycle_error_kind,
            },
            "opportunities": [
                {
                    **item.to_dict(),
                    "opportunity_id": durable_ids.get(item.group_id),
                    "lifecycle_status": (
                        "available" if item.group_id in durable_ids else lifecycle_status
                    ),
                    "execution_status": "not-verified",
                    "snapshot_age_seconds": universe_age_seconds,
                    "quote_age_seconds": quote_age_seconds,
                    "universe_age_seconds": universe_age_seconds,
                }
                for item in selected
            ],
        }
    )


async def opportunities(request: Request) -> JSONResponse:
    """Serve within one absolute budget even when isolated reads become zombies."""
    try:
        return await asyncio.wait_for(
            _opportunities(request),
            timeout=_ENDPOINT_TIMEOUT_S,
        )
    except TimeoutError:
        return JSONResponse(
            {"error": "verified market universe unavailable"},
            status_code=503,
        )
