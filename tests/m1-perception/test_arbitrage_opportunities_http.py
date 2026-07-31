from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from types import SimpleNamespace

from polyarb.daemon.quote_worker import QuoteWorkerRuntime
from polyarb.routing.neg_risk_quote_store import (
    QuoteProjectionIntegrityError,
    QuoteUniverseUnavailableError,
)
from polyarb.routing.opportunity_scanner import (
    OpportunityScanResult,
    QuoteRunUnavailableError,
    StaleQuoteRunError,
    StaleUniverseError,
)


@dataclass
class _Opportunity:
    group_id: str = "group-1"
    gross_edge_bps: float = 250.0

    def to_dict(self) -> dict:
        return {
            "group_id": self.group_id,
            "gross_edge_bps": self.gross_edge_bps,
        }


NOW_S = 1_800_000_000.0


def _publish_feed(
    runtime: QuoteWorkerRuntime,
    *,
    opportunities=(_Opportunity(),),
) -> None:
    projection = SimpleNamespace(
        run_id=20,
        universe_snapshot_id=10,
        quoted_at_ms=int(NOW_S * 1000),
        universe_taken_at_ms=int(NOW_S * 1000),
        requested_token_count=1,
        successful_response_count=1,
        universe_hash="u1",
        source_truth_hash="truth-1",
    )
    result = OpportunityScanResult(
        opportunities=opportunities,
        rejections={"augmented-neg-risk-not-supported": 4},
        source_snapshot_id=10,
        universe_hash="u1",
        quote_run_id=20,
    )
    runtime.publish_certified_feed(projection, result)


def test_opportunity_endpoint_returns_explicit_gross_basis(http_test_client, monkeypatch) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._last_complete_snapshot_id",
        lambda _path: 10,
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    response = http_test_client.get("/arbitrage/opportunities?min_edge_bps=100")

    assert response.status_code == 200
    assert response.json() == {
        "strategy": "neg-risk-buy-all",
        "profit_basis": "gross-before-fees",
        "coverage": "verified-standard-neg-risk",
        "source_snapshot_id": 10,
        "universe_hash": "u1",
        "quote_run_id": 20,
        "quote_sla_seconds": 300,
        "count": 1,
        "rejections": {"augmented-neg-risk-not-supported": 4},
        "opportunities": [
            {
                "group_id": "group-1",
                "gross_edge_bps": 250.0,
                "opportunity_id": None,
                "execution_status": "not-verified",
                "snapshot_age_seconds": 0.0,
                "quote_age_seconds": 0.0,
                "universe_age_seconds": 0.0,
            }
        ],
    }


def test_legacy_feed_does_not_attach_an_old_observer_after_new_structure_publishes(
    http_test_client, monkeypatch
) -> None:
    now_ms = int(NOW_S * 1000)
    db_path = http_test_client.app.state.sqlite_store.db_path
    with sqlite3.connect(db_path) as con:
        for revision in (10, 11):
            con.execute(
                "INSERT INTO snapshots("
                "id,taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
                "data_product,archive_status,snapshot_status,is_valid,parquet_path"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    revision,
                    now_ms,
                    now_ms,
                    "full",
                    1,
                    1,
                    "structure",
                    "not-requested",
                    "ok",
                    1,
                    "",
                ),
            )
            con.execute(
                "INSERT INTO snapshot_source_coverage("
                "snapshot_id,completed,market_items,event_items,failure_source,failure_reason"
                ") VALUES (?,1,1,1,NULL,NULL)",
                (revision,),
            )
            con.execute(
                "INSERT INTO neg_risk_group_truth("
                "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
                "expected_member_count,active_named_count,membership_hash,quality,reason"
                ") VALUES (?, 'event-1','group-1','standard',1,1,'membership-1',"
                "'complete-supported',NULL)",
                (revision,),
            )
        con.execute(
            "INSERT INTO neg_risk_quote_runs("
            "id,universe_snapshot_id,universe_taken_at_ms,universe_hash,source_truth_hash,"
            "quoted_at_ms,requested_token_count,successful_response_count,lease_expires_at_ms,"
            "status,failure_reason,completed_at_ms"
            ") VALUES (20,10,?,'u1','truth-1',?,1,1,0,'complete',NULL,?)",
            (now_ms, now_ms, now_ms),
        )
        con.execute(
            "INSERT INTO neg_risk_opportunities("
            "id,event_id,group_id,membership_hash,status,bundle_cost,gross_edge_bps,"
            "max_bundle_size,structure_revision,quote_run_id,opened_at_ms,updated_at_ms"
            ") VALUES ('old-observer','event-1','group-1','membership-1','observe',"
            "0.95,500,1,10,20,?,?)",
            (now_ms, now_ms),
        )

    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr("polyarb.http.arbitrage._last_complete_snapshot_id", lambda _path: 10)
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 200
    assert response.json()["opportunities"][0]["opportunity_id"] is None


def test_opportunity_endpoint_rejects_non_finite_threshold(http_test_client) -> None:
    response = http_test_client.get("/arbitrage/opportunities?min_edge_bps=nan")

    assert response.status_code == 400
    assert response.json() == {"error": "invalid numeric query"}


def test_opportunity_endpoint_rejects_negative_threshold_as_caller_error(
    http_test_client,
) -> None:
    response = http_test_client.get("/arbitrage/opportunities?min_edge_bps=-1")

    assert response.status_code == 400
    assert response.json() == {"error": "invalid numeric query"}


def test_opportunity_endpoint_returns_bounded_503_for_quote_run_preconditions(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._last_complete_snapshot_id",
        lambda _path: 10,
    )
    cases = [
        (
            QuoteUniverseUnavailableError("source coverage incomplete"),
            "verified market universe unavailable",
        ),
        (
            QuoteProjectionIntegrityError(),
            "verified market universe unavailable",
        ),
        (QuoteRunUnavailableError("quote run unavailable"), "verified market universe unavailable"),
        (StaleQuoteRunError("quote age 300.1s exceeds 300.0s"), "quote age 300.1s exceeds 300.0s"),
        (
            StaleUniverseError("universe age 50400.1s exceeds 50400.0s"),
            "universe age 50400.1s exceeds 50400.0s",
        ),
    ]
    for error, expected in cases:
        monkeypatch.setattr(
            "polyarb.http.arbitrage._select_cached_opportunities",
            lambda *_args, error=error, **_kwargs: (_ for _ in ()).throw(error),
        )

        response = http_test_client.get("/arbitrage/opportunities")

        assert response.status_code == 503
        assert response.json() == {"error": expected}


def test_opportunity_endpoint_cold_cache_fails_without_database_scan(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    http_test_client.app.state.quote_worker_runtime = runtime

    def forbidden(*_args, **_kwargs):
        raise AssertionError("endpoint must not rebuild a certified projection")

    monkeypatch.setattr(
        "polyarb.routing.opportunity_scanner.scan_certified_neg_risk_quote_projection",
        forbidden,
    )

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 503
    assert response.json() == {"error": "verified market universe unavailable"}


def test_opportunity_endpoint_fails_closed_when_market_truth_advances(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._last_complete_snapshot_id",
        lambda _path: 11,
    )

    def forbidden(*_args, **_kwargs):
        raise AssertionError("stale projection must not be scanned")

    monkeypatch.setattr(
        "polyarb.http.arbitrage._select_cached_opportunities",
        forbidden,
    )

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 503
    assert response.json() == {"error": "verified market universe unavailable"}


def test_opportunity_endpoint_bounds_source_truth_read_latency(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime

    async def never_finishes(*_args, **_kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr("polyarb.http.arbitrage.asyncio.to_thread", never_finishes)
    monkeypatch.setattr(
        "polyarb.http.arbitrage._SOURCE_TRUTH_READ_TIMEOUT_S",
        0.01,
    )

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 503
    assert response.json() == {"error": "verified market universe unavailable"}


def test_opportunity_endpoint_filters_precomputed_feed_without_rescan(
    http_test_client,
    monkeypatch,
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(
        runtime,
        opportunities=(
            _Opportunity("high", 300.0),
            _Opportunity("low", 100.0),
        ),
    )
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._last_complete_snapshot_id",
        lambda _path: 10,
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("HTTP must not rescan the full projection")

    monkeypatch.setattr(
        "polyarb.routing.opportunity_scanner.scan_certified_neg_risk_quote_projection",
        forbidden,
    )

    response = http_test_client.get(
        "/arbitrage/opportunities?min_edge_bps=200&limit=1"
    )

    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["opportunities"][0]["group_id"] == "high"
