from __future__ import annotations

import asyncio
import sqlite3
import threading
import time

import pytest

from polyarb.config import Settings
from polyarb.daemon.quote_worker import QuoteWorkerRuntime
from polyarb.http.health import _build_health_checks
from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult
from polyarb.routing.neg_risk_quote_store import (
    NegRiskQuoteStore,
    PersistedQuote,
    UniverseLeg,
)
from polyarb.storage.sqlite_store import SQLiteStore

NOW_S = 1_800_000_000.0
NOW_MS = int(NOW_S * 1000)


def _settings(tmp_path, *, enabled: bool) -> Settings:
    return Settings(
        db_path=tmp_path / "state.db",
        neg_risk_quote_worker_enabled=enabled,
        supabase_url="",
        r2_endpoint="",
    )


def _complete_run(settings: Settings, *, age_s: float) -> None:
    SQLiteStore(settings.db_path).init_schema()
    with sqlite3.connect(settings.db_path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,archive_status,is_valid,parquet_path"
            ") VALUES (?,?,'subset',1,1,'structure','not_requested',1,'fixture.parquet')",
            (NOW_MS - 1_000, NOW_MS - 900),
        )
        snapshot_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.execute(
            "INSERT INTO markets("
            "market_id, condition_id, slug, yes_token_id, active, closed, "
            "neg_risk_market_id, fetched_at_ms, snapshot_id,event_id,incomplete"
            ") VALUES ('market-a', 'condition-a', 'alpha', 'token-a', "
            "1, 0, 'group-a', ?, ?,'event-a',0)",
            (NOW_MS, snapshot_id),
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items"
            ") VALUES (?,1,1,1)",
            (snapshot_id,),
        )
        con.execute(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,active,closed"
            ") VALUES (?,'event-a','group-a','market-a','named',1,0)",
            (snapshot_id,),
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (?,'event-a','group-a','standard',1,1,'membership-a',"
            "'complete-supported',NULL)",
            (snapshot_id,),
        )
    store = NegRiskQuoteStore(settings.db_path, now_ms=lambda: NOW_MS)
    leg = UniverseLeg(
        "group-a",
        "market-a",
        "condition-a",
        "alpha",
        "token-a",
        "event-a",
        "membership-a",
    )
    run_id = store.begin_run(
        universe_snapshot_id=snapshot_id,
        universe_taken_at_ms=NOW_MS - 1_000,
        legs=(leg,),
        quoted_at_ms=int((NOW_S - age_s) * 1000),
    )
    store.record_terminal_quotes(
        run_id,
        (
            PersistedQuote(
                "group-a",
                "market-a",
                "condition-a",
                "alpha",
                "token-a",
                "executable",
                0.4,
                10.0,
                "event-a",
                "membership-a",
            ),
        ),
    )
    store.complete_run(
        run_id,
        completed_at_ms=NOW_MS,
        successful_response_count=1,
    )


def _quote_check(settings: Settings, *, runtime=None):
    store = SQLiteStore(settings.db_path)
    # Production initializes once before serving health.  Keep setup outside
    # the latency assertion when this fixture already created the database.
    if not settings.db_path.exists():
        store.init_schema()
    if settings.neg_risk_quote_worker_enabled and runtime is None:
        runtime = QuoteWorkerRuntime()
        projection = NegRiskQuoteStore(settings.db_path).latest_complete_projection()
        if projection is not None:
            runtime.publish_certified_projection(projection)
            runtime.state = "pass"
    return _build_health_checks(store, settings, NOW_S, runtime)


def test_enabled_health_fails_when_no_complete_run(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=True)

    checks, overall = _quote_check(settings)

    entry = checks["quote_feed:last_complete_age_seconds"][0]
    assert entry["observedValue"] is None
    assert entry["status"] == "fail"
    assert overall == "fail"


def test_enabled_health_cold_cache_never_reads_full_projection(tmp_path, monkeypatch) -> None:
    settings = _settings(tmp_path, enabled=True)
    store = SQLiteStore(settings.db_path)
    store.init_schema()
    runtime = QuoteWorkerRuntime()

    def unreadable(_self):
        raise AssertionError("health must not rebuild a certified projection")

    monkeypatch.setattr(NegRiskQuoteStore, "latest_complete_projection", unreadable)

    checks, overall = _build_health_checks(store, settings, NOW_S, runtime)

    entry = checks["quote_feed:last_complete_age_seconds"][0]
    assert entry["observedValue"] is None
    assert entry["status"] == "fail"
    assert entry["output"] == "certified-projection-unavailable"
    assert overall == "fail"


@pytest.mark.parametrize(
    ("age_s", "expected"),
    ((239.0, "pass"), (240.0, "warn"), (300.0, "warn"), (300.1, "fail")),
)
def test_quote_age_boundaries(tmp_path, age_s: float, expected: str) -> None:
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=age_s)

    checks, overall = _quote_check(settings)

    entry = checks["quote_feed:last_complete_age_seconds"][0]
    assert entry["observedValue"] == age_s
    assert entry["status"] == expected
    assert overall == expected


def test_collecting_quote_phase_checkpoint_fails_after_120_seconds(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    attempt_store = NegRiskQuoteStore(
        settings.db_path,
        now_ms=lambda: NOW_MS - 121_000,
    )
    attempt_store.start_collection_attempt()

    checks, overall = _quote_check(settings)

    phase = checks["quote_feed:collection_phase"][0]
    assert phase["observedValue"] == 121.0
    assert phase["status"] == "fail"
    assert "phase=universe" in phase["output"]
    assert overall == "fail"


def test_worker_error_warns_while_complete_run_is_fresh(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    runtime = QuoteWorkerRuntime()
    projection = NegRiskQuoteStore(settings.db_path).latest_complete_projection()
    assert projection is not None
    runtime.publish_certified_projection(projection)
    runtime.mark_started()
    runtime.mark_failure(RuntimeError("must not be exposed"))

    checks, overall = _quote_check(settings, runtime=runtime)

    collector = checks["quote_feed:collector_state"][0]
    assert collector["observedValue"] == "error"
    assert collector["status"] == "warn"
    assert collector["output"] == "RuntimeError"
    assert overall == "warn"


def test_quote_retention_failure_is_health_visible(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    runtime = QuoteWorkerRuntime()
    projection = NegRiskQuoteStore(settings.db_path).latest_complete_projection()
    assert projection is not None
    runtime.publish_certified_projection(projection)
    runtime.mark_cleanup_failure(OSError("disk cleanup failed"))

    checks, overall = _quote_check(settings, runtime=runtime)

    retention = checks["quote_feed:retention"][0]
    assert retention["observedValue"] == 1
    assert retention["status"] == "warn"
    assert retention["output"] == "OSError"
    assert overall == "warn"


def test_repeated_quote_retention_failure_fails_health(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    runtime = QuoteWorkerRuntime()
    projection = NegRiskQuoteStore(settings.db_path).latest_complete_projection()
    assert projection is not None
    runtime.publish_certified_projection(projection)
    for _ in range(3):
        runtime.mark_cleanup_failure(OSError("disk cleanup failed"))

    checks, overall = _quote_check(settings, runtime=runtime)

    assert checks["quote_feed:retention"][0]["status"] == "fail"
    assert overall == "fail"


def test_new_quote_attempt_does_not_report_a_previous_error_as_current() -> None:
    """A current re-quote is collecting; its old failure stays only in counters."""
    runtime = QuoteWorkerRuntime()
    runtime.mark_started()
    runtime.mark_failure(RuntimeError("previous attempt"))
    runtime.mark_started()

    snapshot = runtime.snapshot()

    assert snapshot.state == "collecting"
    assert snapshot.failure_count == 1
    assert snapshot.last_error_kind is None


def test_enabled_health_fails_when_source_truth_drifts(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    runtime = QuoteWorkerRuntime()
    projection = NegRiskQuoteStore(settings.db_path).latest_complete_projection()
    assert projection is not None
    runtime.publish_certified_projection(projection)
    with sqlite3.connect(settings.db_path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,archive_status,is_valid,parquet_path"
            ") VALUES (?,?,'subset',1,1,'structure','not_requested',1,'new.parquet')",
            (NOW_MS - 301_000, NOW_MS - 301_000),
        )
        snapshot_id = int(
            con.execute("SELECT id FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()[0]
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items"
            ") VALUES (?,1,1,1)",
            (snapshot_id,),
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (?,'event-z','group-z','augmented',1,1,'membership-z',"
            "'complete-unsupported','augmented-neg-risk-not-supported')",
            (snapshot_id,),
        )

    checks, overall = _quote_check(settings, runtime=runtime)

    entry = checks["quote_feed:last_complete_age_seconds"][0]
    assert entry["observedValue"] == 10.0
    assert entry["status"] == "fail"
    assert entry["output"] == "source-snapshot-mismatch"
    assert overall == "fail"


def test_fresh_structure_publish_warns_before_quote_worker_wakes(tmp_path) -> None:
    """Publication-to-request scheduling is part of the bounded refresh window."""
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    runtime = QuoteWorkerRuntime()
    projection = NegRiskQuoteStore(settings.db_path).latest_complete_projection()
    assert projection is not None
    runtime.publish_certified_projection(projection)
    with sqlite3.connect(settings.db_path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,archive_status,is_valid,parquet_path"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            (NOW_MS, NOW_MS, "subset", 1, 1, "structure", "not_requested", 1, "new.parquet"),
        )
        snapshot_id = int(
            con.execute("SELECT id FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()[0]
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items"
            ") VALUES (?,1,1,1)",
            (snapshot_id,),
        )

    checks, overall = _quote_check(settings, runtime=runtime)

    entry = checks["quote_feed:last_complete_age_seconds"][0]
    assert entry["observedValue"] == 10.0
    assert entry["status"] == "warn"
    assert entry["output"] == "source-snapshot-refreshing-serving-previous"
    assert overall == "warn"


def test_collecting_worker_warns_while_current_structure_requotes(tmp_path) -> None:
    """A fresh Structure waits boundedly for its matching Quote, rather than lying."""
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    runtime = QuoteWorkerRuntime()
    projection = NegRiskQuoteStore(settings.db_path).latest_complete_projection()
    assert projection is not None
    runtime.publish_certified_projection(projection)
    runtime.mark_started()
    with sqlite3.connect(settings.db_path) as con:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,archive_status,is_valid,parquet_path"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            (NOW_MS, NOW_MS, "subset", 1, 1, "structure", "not_requested", 1, "new.parquet"),
        )
        snapshot_id = int(
            con.execute("SELECT id FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()[0]
        )
        con.execute(
            "INSERT INTO snapshot_source_coverage("
            "snapshot_id,completed,market_items,event_items"
            ") VALUES (?,1,1,1)",
            (snapshot_id,),
        )
        con.execute(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (?,'event-z','group-z','augmented',1,1,'membership-z',"
            "'complete-unsupported','augmented-neg-risk-not-supported')",
            (snapshot_id,),
        )

    checks, overall = _quote_check(settings, runtime=runtime)

    entry = checks["quote_feed:last_complete_age_seconds"][0]
    assert entry["observedValue"] == 10.0
    assert entry["status"] == "warn"
    assert entry["output"] == "source-snapshot-refreshing-serving-previous"
    assert overall == "warn"


def test_disabled_worker_registers_no_quote_checks(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=False)

    checks, overall = _quote_check(settings)

    assert not any(name.startswith("quote_feed:") for name in checks)
    assert overall == "fail"  # no snapshot is still truthful


async def test_health_reads_previous_cache_while_next_projection_certifies(
    tmp_path,
    monkeypatch,
) -> None:
    from polyarb.daemon.quote_worker import certify_latest_quote_projection

    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    quote_store = NegRiskQuoteStore(settings.db_path)
    projection = quote_store.latest_complete_projection()
    assert projection is not None
    runtime = QuoteWorkerRuntime()
    runtime.publish_certified_projection(projection)
    runtime.state = "collecting"
    started = threading.Event()
    release = threading.Event()

    def slow_certification(_self):
        started.set()
        assert release.wait(timeout=1)
        return projection

    monkeypatch.setattr(
        NegRiskQuoteStore,
        "latest_complete_projection",
        slow_certification,
    )
    result = QuoteCollectionResult(
        run_id=projection.run_id,
        status="complete",
        universe_snapshot_id=projection.universe_snapshot_id,
        requested_token_count=projection.requested_token_count,
        successful_response_count=projection.successful_response_count,
        quote_taken_at_ms=projection.quoted_at_ms,
        elapsed_ms=25,
    )
    certification = asyncio.create_task(
        certify_latest_quote_projection(quote_store, result)
    )
    assert await asyncio.to_thread(started.wait, 0.2)

    before = time.perf_counter()
    checks, overall = _quote_check(settings, runtime=runtime)
    elapsed = time.perf_counter() - before

    assert elapsed < 0.1
    assert checks["quote_feed:last_complete_age_seconds"][0]["status"] == "pass"
    assert overall == "pass"
    assert not certification.done()
    release.set()
    assert await certification is projection
