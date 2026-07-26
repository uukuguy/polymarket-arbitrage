from __future__ import annotations

import sqlite3

import pytest

from polyarb.config import Settings
from polyarb.daemon.quote_worker import QuoteWorkerRuntime
from polyarb.http.health import _build_health_checks
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
            "taken_at_ms, finished_at_ms, mode, market_count, is_valid, parquet_path"
            ") VALUES (?, ?, 'subset', 1, 1, 'fixture.parquet')",
            (NOW_MS - 1_000, NOW_MS - 900),
        )
        snapshot_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
        con.execute(
            "INSERT INTO markets("
            "market_id, condition_id, slug, yes_token_id, active, closed, "
            "neg_risk_market_id, fetched_at_ms, snapshot_id"
            ") VALUES ('market-a', 'condition-a', 'alpha', 'token-a', "
            "1, 0, 'group-a', ?, ?)",
            (NOW_MS, snapshot_id),
        )
    store = NegRiskQuoteStore(settings.db_path, now_ms=lambda: NOW_MS)
    leg = UniverseLeg("group-a", "market-a", "condition-a", "alpha", "token-a")
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
    store.init_schema()
    return _build_health_checks(store, settings, NOW_S, runtime)


def test_enabled_health_fails_when_no_complete_run(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=True)

    checks, overall = _quote_check(settings)

    entry = checks["quote_feed:last_complete_age_seconds"][0]
    assert entry["observedValue"] is None
    assert entry["status"] == "fail"
    assert overall == "fail"


def test_enabled_health_fails_closed_when_quote_store_is_unreadable(
    tmp_path, monkeypatch
) -> None:
    settings = _settings(tmp_path, enabled=True)
    store = SQLiteStore(settings.db_path)
    store.init_schema()

    def unreadable(_self):
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(NegRiskQuoteStore, "latest_complete_run", unreadable)

    checks, overall = _build_health_checks(store, settings, NOW_S)

    entry = checks["quote_feed:last_complete_age_seconds"][0]
    assert entry["observedValue"] is None
    assert entry["status"] == "fail"
    assert entry["output"] == "quote-store-unreadable:OperationalError"
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


def test_worker_error_warns_while_complete_run_is_fresh(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    runtime = QuoteWorkerRuntime()
    runtime.mark_started()
    runtime.mark_failure(RuntimeError("must not be exposed"))

    checks, overall = _quote_check(settings, runtime=runtime)

    collector = checks["quote_feed:collector_state"][0]
    assert collector["observedValue"] == "error"
    assert collector["status"] == "warn"
    assert collector["output"] == "RuntimeError"
    assert overall == "warn"


def test_disabled_worker_registers_no_quote_checks(tmp_path) -> None:
    settings = _settings(tmp_path, enabled=False)

    checks, overall = _quote_check(settings)

    assert not any(name.startswith("quote_feed:") for name in checks)
    assert overall == "fail"  # no snapshot is still truthful
