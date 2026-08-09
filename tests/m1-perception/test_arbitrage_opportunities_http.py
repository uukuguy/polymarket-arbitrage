from __future__ import annotations

import asyncio
import sqlite3
import threading
import time
from dataclasses import dataclass, replace
from types import SimpleNamespace

import pytest

from polyarb.daemon.quote_worker import QuoteWorkerRuntime
from polyarb.http.opportunity_read_health import (
    BoundedReadLane,
    OpportunityReadHealth,
    ReadLaneClosedError,
    ReadLaneSaturatedError,
)
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
UNIVERSE_HASH = "a" * 64
SOURCE_TRUTH_HASH = "b" * 64


def _publish_feed(
    runtime: QuoteWorkerRuntime,
    *,
    opportunities=(_Opportunity(),),
    snapshot_id: int = 10,
    run_id: int = 20,
) -> None:
    projection = SimpleNamespace(
        run_id=run_id,
        universe_snapshot_id=snapshot_id,
        quoted_at_ms=int(NOW_S * 1000),
        universe_taken_at_ms=int(NOW_S * 1000),
        requested_token_count=1,
        successful_response_count=1,
        universe_hash=UNIVERSE_HASH,
        source_truth_hash=SOURCE_TRUTH_HASH,
    )
    result = OpportunityScanResult(
        opportunities=opportunities,
        rejections={"augmented-neg-risk-not-supported": 4},
        source_snapshot_id=snapshot_id,
        universe_hash=UNIVERSE_HASH,
        quote_run_id=run_id,
    )
    runtime.publish_certified_feed(projection, result)


def _truth(snapshot_id: int | None, age_s: float = 0.0):
    return SimpleNamespace(
        last_complete_snapshot_id=snapshot_id,
        last_complete_finished_age_seconds=age_s,
    )


def test_opportunity_endpoint_returns_explicit_gross_basis(http_test_client, monkeypatch) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    response = http_test_client.get("/arbitrage/opportunities?min_edge_bps=100")

    assert response.status_code == 200
    assert response.json() == {
        "strategy": "neg-risk-buy-all",
        "profit_basis": "gross-before-fees",
        "coverage": "verified-standard-neg-risk",
        "refreshing": False,
        "source_truth_status": "live",
        "source_truth_live_available": True,
        "latest_structure_snapshot_id": 10,
        "source_snapshot_id": 10,
        "universe_hash": UNIVERSE_HASH,
        "quote_run_id": 20,
        "quote_sla_seconds": 300,
        "count": 1,
        "rejections": {"augmented-neg-risk-not-supported": 4},
        "read_diagnostics": {
            "source_truth_status": "live",
            "source_truth_error_kind": None,
            "lifecycle_status": "pending",
            "lifecycle_error_kind": None,
        },
        "opportunities": [
            {
                "group_id": "group-1",
                "gross_edge_bps": 250.0,
                "opportunity_id": None,
                "lifecycle_status": "pending",
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
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
    )
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
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
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


def test_opportunity_endpoint_hydrates_certified_feed_from_isolated_producer(
    http_test_client, monkeypatch
) -> None:
    """HTTP owns no producer runtime when the Quote worker is supervised elsewhere."""
    resident_runtime = QuoteWorkerRuntime()
    producer_runtime = QuoteWorkerRuntime()
    _publish_feed(producer_runtime)
    http_test_client.app.state.quote_worker_runtime = resident_runtime
    http_test_client.app.state.quote_feed_loader = producer_runtime.certified_feed
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 200
    assert response.json()["quote_run_id"] == 20
    assert resident_runtime.certified_feed() is producer_runtime.certified_feed()


def test_opportunity_endpoint_turns_stale_loader_feed_into_structured_503(
    http_test_client,
) -> None:
    http_test_client.app.state.quote_worker_runtime = QuoteWorkerRuntime()

    def stale_loader():
        raise StaleQuoteRunError("quote age 2662.0s exceeds 300.0s")

    http_test_client.app.state.quote_feed_loader = stale_loader

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 503
    assert response.json() == {"error": "quote age 2662.0s exceeds 300.0s"}


def test_opportunity_endpoint_uses_authenticated_fallback_for_incomplete_source_truth(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: SimpleNamespace(
            coverage_status="fail",
            last_complete_snapshot_id=845,
            last_complete_finished_age_seconds=999_999.0,
        ),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 200
    assert response.json()["source_truth_status"] == "last-known-authenticated"
    assert response.json()["source_snapshot_id"] == 10


def test_opportunity_endpoint_serves_previous_feed_when_market_truth_advances(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(11, 30.0),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("HTTP must not rebuild the certified feed")

    monkeypatch.setattr(
        "polyarb.routing.opportunity_scanner.scan_certified_neg_risk_quote_projection",
        forbidden,
    )

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 200
    assert response.json()["refreshing"] is True
    assert response.json()["latest_structure_snapshot_id"] == 11
    assert response.json()["source_snapshot_id"] == 10
    assert response.json()["quote_run_id"] == 20


@pytest.mark.parametrize(
    ("latest_id", "handoff_age"),
    ((9, 1.0), (11, 300.1)),
)
def test_opportunity_endpoint_rejects_unavailable_revision_handoffs(
    http_test_client,
    monkeypatch,
    latest_id: int | None,
    handoff_age: float,
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(latest_id, handoff_age),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 503
    assert response.json() == {"error": "verified market universe unavailable"}


def test_opportunity_endpoint_atomically_switches_to_new_certified_feed(
    http_test_client,
    monkeypatch,
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime, snapshot_id=10, run_id=20)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(11, 30.0),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    before = http_test_client.get("/arbitrage/opportunities")
    _publish_feed(runtime, snapshot_id=11, run_id=21)
    after = http_test_client.get("/arbitrage/opportunities")

    assert before.status_code == after.status_code == 200
    assert (
        before.json()["source_snapshot_id"],
        before.json()["quote_run_id"],
        before.json()["refreshing"],
    ) == (10, 20, True)
    assert (
        after.json()["source_snapshot_id"],
        after.json()["quote_run_id"],
        after.json()["refreshing"],
    ) == (11, 21, False)


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

    assert response.status_code == 200
    assert response.json()["source_truth_status"] == "last-known-authenticated"


def test_opportunity_endpoint_survives_shared_default_executor_starvation(
    http_test_client, monkeypatch
) -> None:
    """A 116k-row sibling read cannot queue authority work behind its executor."""
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    async def saturated_default_executor(*_args, **_kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(
        "polyarb.http.arbitrage.asyncio.to_thread",
        saturated_default_executor,
    )
    monkeypatch.setattr(
        "polyarb.http.arbitrage._SOURCE_TRUTH_READ_TIMEOUT_S",
        0.01,
    )

    started = time.monotonic()
    response = http_test_client.get("/arbitrage/opportunities")
    elapsed = time.monotonic() - started

    assert elapsed < 3.0
    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["opportunities"][0]["group_id"] == "group-1"


def test_lifecycle_timeout_keeps_every_candidate_and_exposes_diagnostic_truth(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(
        runtime,
        opportunities=(
            _Opportunity("group-1", 250.0),
            _Opportunity("group-2", 200.0),
        ),
    )
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)
    monkeypatch.setattr(
        "polyarb.http.arbitrage._LIFECYCLE_READ_TIMEOUT_S",
        0.01,
        raising=False,
    )
    release = threading.Event()

    def blocked_lifecycle_read(*_args, **_kwargs):
        release.wait(timeout=1.0)
        return {}

    monkeypatch.setattr(
        "polyarb.http.arbitrage.durable_opportunity_ids",
        blocked_lifecycle_read,
    )
    try:
        response = http_test_client.get("/arbitrage/opportunities")
    finally:
        release.set()

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 2
    assert [item["group_id"] for item in payload["opportunities"]] == [
        "group-1",
        "group-2",
    ]
    assert all(item["opportunity_id"] is None for item in payload["opportunities"])
    assert all(item["lifecycle_status"] == "unavailable" for item in payload["opportunities"])
    assert payload["read_diagnostics"]["lifecycle_status"] == "unavailable"
    health = http_test_client.app.state.opportunity_read_health.snapshot()
    assert health["lifecycle_consecutive_failures"] == 1
    assert health["lifecycle_last_error_kind"] == "timeout"


def test_source_truth_timeout_uses_only_matching_authenticated_feed_binding(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)
    monkeypatch.setattr(
        "polyarb.http.arbitrage._SOURCE_TRUTH_READ_TIMEOUT_S",
        0.01,
    )
    release = threading.Event()

    def blocked_truth_read(*_args, **_kwargs):
        release.wait(timeout=1.0)
        return _truth(10)

    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        blocked_truth_read,
    )
    try:
        response = http_test_client.get("/arbitrage/opportunities")
    finally:
        release.set()

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] == 1
    assert payload["source_truth_status"] == "last-known-authenticated"
    assert payload["source_truth_live_available"] is False
    assert payload["latest_structure_snapshot_id"] == 10
    assert payload["read_diagnostics"]["source_truth_error_kind"] == "timeout"
    health = http_test_client.app.state.opportunity_read_health.snapshot()
    assert health["source_truth_consecutive_failures"] == 1
    assert health["source_truth_last_error_kind"] == "timeout"


def test_source_truth_fallback_rejects_mixed_feed_identity(http_test_client, monkeypatch) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    feed = runtime.certified_feed()
    assert feed is not None and feed.opportunity_scan is not None
    runtime._certified_feed = replace(  # noqa: SLF001 - construct corrupted input
        feed,
        opportunity_scan=replace(feed.opportunity_scan, universe_hash="other-universe"),
    )
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)
    monkeypatch.setattr(
        "polyarb.http.arbitrage._SOURCE_TRUTH_READ_TIMEOUT_S",
        0.01,
    )
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda *_args: (_ for _ in ()).throw(sqlite3.OperationalError("busy")),
    )

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 503
    assert response.json() == {"error": "verified market universe unavailable"}


def test_source_truth_fallback_rejects_feed_without_authenticated_hash(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    feed = runtime.certified_feed()
    assert feed is not None
    runtime._certified_feed = replace(  # noqa: SLF001 - construct corrupted input
        feed,
        projection=replace(feed.projection, source_truth_hash=""),
    )
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda *_args: (_ for _ in ()).throw(sqlite3.OperationalError("busy")),
    )

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 503
    assert response.json() == {"error": "verified market universe unavailable"}
    assert (
        http_test_client.app.state.opportunity_read_health.snapshot()[
            "source_truth_status"
        ]
        == "authentication-invalid"
    )


@pytest.mark.asyncio
async def test_bounded_read_lane_rejects_zombie_queueing_and_recovers() -> None:
    lane = BoundedReadLane("test-opportunity-read", capacity=1)
    release = threading.Event()

    def blocked() -> str:
        release.wait(timeout=1.0)
        return "released"

    with pytest.raises(TimeoutError):
        await lane.run(blocked, timeout_s=0.01)
    started = time.monotonic()
    with pytest.raises(ReadLaneSaturatedError):
        await lane.run(lambda: "must-not-queue", timeout_s=0.5)
    assert time.monotonic() - started < 0.1

    release.set()
    for _ in range(100):
        try:
            result = await lane.run(lambda: "recovered", timeout_s=0.5)
        except ReadLaneSaturatedError:
            await asyncio.sleep(0)
            continue
        assert result == "recovered"
        break
    else:
        pytest.fail("read lane did not recover after timed-out worker completed")


def test_read_health_rejects_stale_completion_in_both_orderings() -> None:
    registry = OpportunityReadHealth()

    old = registry.begin_source_attempt(100.0)
    latest = registry.begin_source_attempt(101.0)
    assert registry.mark_source_live(latest, 102.0) is True
    assert registry.mark_source_fallback(old, 103.0, "timeout") is False
    snapshot = registry.snapshot()
    assert snapshot["source_truth_status"] == "live"
    assert snapshot["source_truth_last_attempt_at_s"] == 101.0

    old = registry.begin_source_attempt(104.0)
    latest = registry.begin_source_attempt(105.0)
    assert registry.mark_source_fallback(latest, 106.0, "timeout") is True
    assert registry.mark_source_live(old, 107.0) is False
    snapshot = registry.snapshot()
    assert snapshot["source_truth_status"] == "last-known-authenticated"
    assert snapshot["source_truth_last_attempt_at_s"] == 105.0

    old = registry.begin_lifecycle_attempt(106.0)
    latest = registry.begin_lifecycle_attempt(107.0)
    assert registry.mark_lifecycle(latest, 108.0, "available", None) is True
    assert registry.mark_lifecycle(old, 109.0, "unavailable", "timeout") is False
    snapshot = registry.snapshot()
    assert snapshot["lifecycle_status"] == "available"
    assert snapshot["lifecycle_last_attempt_at_s"] == 107.0


@pytest.mark.parametrize("newer_outcome", ["live", "fallback"])
def test_concurrent_source_completions_cannot_overwrite_newer_request(
    http_test_client,
    monkeypatch,
    newer_outcome: str,
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)
    monkeypatch.setattr(
        "polyarb.http.arbitrage._SOURCE_TRUTH_READ_TIMEOUT_S",
        0.05 if newer_outcome == "live" else 0.5,
    )
    first_started = threading.Event()
    release_first = threading.Event()
    call_lock = threading.Lock()
    call_count = 0

    def ordered_truth(*_args):
        nonlocal call_count
        with call_lock:
            call_count += 1
            call_number = call_count
        if call_number == 1:
            first_started.set()
            release_first.wait(timeout=1.0)
            return _truth(10)
        if newer_outcome == "fallback":
            raise sqlite3.OperationalError("busy")
        return _truth(10)

    monkeypatch.setattr("polyarb.http.arbitrage._market_truth", ordered_truth)
    first_response: list[object] = []
    first_thread = threading.Thread(
        target=lambda: first_response.append(
            http_test_client.get("/arbitrage/opportunities")
        )
    )
    first_thread.start()
    assert first_started.wait(timeout=1.0)
    second = http_test_client.get("/arbitrage/opportunities")
    if newer_outcome == "fallback":
        release_first.set()
    first_thread.join(timeout=1.0)
    release_first.set()

    assert not first_thread.is_alive()
    assert second.status_code == 200
    assert first_response and first_response[0].status_code == 200
    snapshot = http_test_client.app.state.opportunity_read_health.snapshot()
    assert snapshot["source_truth_status"] == (
        "live" if newer_outcome == "live" else "last-known-authenticated"
    )
    assert snapshot["source_truth_latest_token"] >= 2


@pytest.mark.parametrize("newer_outcome", ["available", "unavailable"])
def test_concurrent_lifecycle_completions_cannot_overwrite_newer_request(
    http_test_client,
    monkeypatch,
    newer_outcome: str,
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
    )
    monkeypatch.setattr(
        "polyarb.http.arbitrage._LIFECYCLE_READ_TIMEOUT_S",
        0.05 if newer_outcome == "available" else 0.5,
    )
    first_started = threading.Event()
    release_first = threading.Event()
    call_lock = threading.Lock()
    call_count = 0

    def ordered_lifecycle(*_args, **_kwargs):
        nonlocal call_count
        with call_lock:
            call_count += 1
            call_number = call_count
        if call_number == 1:
            first_started.set()
            release_first.wait(timeout=1.0)
            return {"group-1": "opportunity-old"}
        if newer_outcome == "unavailable":
            raise sqlite3.OperationalError("busy")
        return {"group-1": "opportunity-new"}

    monkeypatch.setattr(
        "polyarb.http.arbitrage.durable_opportunity_ids",
        ordered_lifecycle,
    )
    first_response: list[object] = []
    first_thread = threading.Thread(
        target=lambda: first_response.append(
            http_test_client.get("/arbitrage/opportunities")
        )
    )
    first_thread.start()
    assert first_started.wait(timeout=1.0)
    second = http_test_client.get("/arbitrage/opportunities")
    if newer_outcome == "unavailable":
        release_first.set()
    first_thread.join(timeout=1.0)
    release_first.set()

    assert not first_thread.is_alive()
    assert second.status_code == 200
    assert first_response and first_response[0].status_code == 200
    snapshot = http_test_client.app.state.opportunity_read_health.snapshot()
    assert snapshot["lifecycle_status"] == newer_outcome
    assert snapshot["lifecycle_latest_token"] >= 2


@pytest.mark.asyncio
async def test_app_shutdown_abandons_zombie_lane_and_recreate_is_clean(
    http_test_client,
) -> None:
    from polyarb.http.app import create_app

    app = http_test_client.app
    old_lane = app.state.opportunity_source_truth_lane
    release = threading.Event()

    def zombie() -> None:
        release.wait(timeout=1.0)

    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    with pytest.raises(TimeoutError):
        await old_lane.run(zombie, timeout_s=0.01)
    started = time.monotonic()
    await lifespan.__aexit__(None, None, None)
    assert time.monotonic() - started < 0.1
    old_lane.shutdown()
    old_lane.shutdown()
    with pytest.raises(ReadLaneClosedError):
        await old_lane.run(lambda: None, timeout_s=0.1)

    replacement = create_app(
        scheduler=app.state.scheduler,
        sqlite_store=app.state.sqlite_store,
        settings=app.state.settings,
    )
    new_lane = replacement.state.opportunity_source_truth_lane
    assert new_lane is not old_lane
    async with replacement.router.lifespan_context(replacement):
        assert await new_lane.run(lambda: "ok", timeout_s=0.1) == "ok"
    release.set()


def test_opportunity_read_diagnostics_recover_after_transient_failures(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda *_args: (_ for _ in ()).throw(sqlite3.OperationalError("busy")),
    )
    monkeypatch.setattr(
        "polyarb.http.arbitrage.durable_opportunity_ids",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(sqlite3.OperationalError("busy")),
    )

    degraded = http_test_client.get("/arbitrage/opportunities")
    assert degraded.status_code == 200
    assert degraded.json()["source_truth_status"] == "last-known-authenticated"
    assert degraded.json()["opportunities"][0]["lifecycle_status"] == "unavailable"

    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
    )
    monkeypatch.setattr(
        "polyarb.http.arbitrage.durable_opportunity_ids",
        lambda *_args, **_kwargs: {"group-1": "opportunity-1"},
    )
    recovered = http_test_client.get("/arbitrage/opportunities")

    assert recovered.status_code == 200
    payload = recovered.json()
    assert payload["source_truth_status"] == "live"
    assert payload["opportunities"][0]["lifecycle_status"] == "available"
    assert payload["opportunities"][0]["opportunity_id"] == "opportunity-1"
    health = http_test_client.app.state.opportunity_read_health.snapshot()
    assert health["source_truth_consecutive_failures"] == 0
    assert health["lifecycle_consecutive_failures"] == 0


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
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
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
